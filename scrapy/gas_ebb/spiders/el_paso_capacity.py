import io
import zipfile
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import scrapy
from gas_ebb.items import CapacityItem

XLSX_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

HEADER_MAP = {
    "TSP": "TSP",
    "TSP Name": "TSP_Name",
    "Cycle": "Cycle_Desc",
    "Cycle Desc": "Cycle_Desc",
    "Cycle Selection": "Cycle_Desc",
    "Post Date": "Post_Date",
    "Posting Date": "Post_Date",
    "Post Time": "Post_Time",
    "Posting Time": "Post_Time",
    "Eff Gas Day": "Eff_Gas_Day",
    "Effective Gas Day": "Eff_Gas_Day",
    "Eff Time": "Eff_Time",
    "Effective Time": "Eff_Time",
    "Loc": "Loc",
    "Loc ID": "Loc",
    "Loc Name": "Loc_Name",
    "Loc Zn": "Loc_Zn",
    "Zone": "Loc_Zn",
    "Flow Ind": "Flow_Ind_Desc",
    "Flow Indicator": "Flow_Ind_Desc",
    "Loc Purp Desc": "Loc_Purp_Desc",
    "Loc QTI": "Loc_QTI_Desc",
    "QTI": "Loc_QTI_Desc",
    "Meas Basis Desc": "Meas_Basis_Desc",
    "IT": "IT",
    "IT Indicator": "IT",
    "All Qty Avail": "All_Qty_Avail",
    "Design Capacity": "Total_Design_Capacity",
    "Total Design Capacity": "Total_Design_Capacity",
    "Operating Capacity": "Operating_Capacity",
    "Scheduled Qty": "Total_Scheduled_Quantity",
    "Total Scheduled Quantity": "Total_Scheduled_Quantity",
    "Operationally Available Capacity": "Operationally_Available_Capacity",
    "Available Qty": "Operationally_Available_Capacity",
    "Qty Reason": "Qty_Reason",
}


def _normalize_header(value: str) -> str:
    return " ".join((value or "").replace("\xa0", " ").split())


def _clean_cell(value: str) -> str:
    return " ".join((value or "").replace("\xa0", " ").split()).strip()


def _xlsx_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []

    shared = []
    for si in root.findall("a:si", XLSX_NS):
        text = "".join(node.text or "" for node in si.findall(".//a:t", XLSX_NS))
        shared.append(text)
    return shared


def _xlsx_rows(payload: bytes) -> list[list[str]]:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        shared = _xlsx_shared_strings(zf)
        root = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))

        rows = []
        for row in root.findall(".//a:sheetData/a:row", XLSX_NS):
            current = []
            for cell in row.findall("a:c", XLSX_NS):
                cell_type = cell.get("t")
                value = cell.findtext("a:v", default="", namespaces=XLSX_NS)
                if cell_type == "s" and value:
                    idx = int(value)
                    current.append(shared[idx] if idx < len(shared) else "")
                elif cell_type == "inlineStr":
                    current.append(
                        "".join(
                            node.text or ""
                            for node in cell.findall(".//a:t", XLSX_NS)
                        )
                    )
                else:
                    current.append(value or "")
            rows.append(current)
        return rows


def _datepicker_client_state(target_date) -> str:
    return (
        f'["{target_date.year}-{target_date.month - 1}-{target_date.day}-0-0-0-0,,"'
        ',"04/08/03","*  0322","04/08/03",29,0,"_ig_def_dp_cal","",'
        '"500,3,300,3,0,200,3,100,3,0"]'
    )


class ElPasoCapacitySpider(scrapy.Spider):
    name = "elpaso_capacity"
    allowed_domains = ["pipeline2.kindermorgan.com"]
    start_url = "https://pipeline2.kindermorgan.com/Capacity/OpAvailSegment.aspx?code=EPGD"
    mongo_collection = "ebb_el_paso_capacity"
    mongo_unique_fields = ["Loc", "Post_Date", "Post_Time", "Cycle_Desc", "TSP"]
    custom_settings = {"DOWNLOAD_TIMEOUT": 120}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raw_days_ago = kwargs.get("days_ago", "1")
        try:
            self.days_ago = max(1, int(raw_days_ago))
        except ValueError:
            self.days_ago = 1

    async def start(self):
        for request in self._build_initial_requests():
            yield request

    def start_requests(self):
        yield from self._build_initial_requests()

    def _build_initial_requests(self):
        today_utc = datetime.now(timezone.utc).date()
        cookiejar_id = 0

        for offset in range(self.days_ago):
            target_date = today_utc - timedelta(days=offset)
            yield scrapy.Request(
                self.start_url,
                callback=self.parse_page,
                meta={
                    "target_date": target_date,
                    "cookiejar": cookiejar_id,
                },
                dont_filter=True,
            )
            cookiejar_id += 1

    def parse_page(self, response):
        yield scrapy.FormRequest.from_response(
            response,
            formid="Form1",
            formdata={
                "WebSplitter1$tmpl1$ContentPlaceHolder1$dtePickerBegin_clientState": _datepicker_client_state(
                    response.meta["target_date"]
                ),
            },
            clickdata={
                "name": "ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve"
            },
            callback=self.parse_retrieved_page,
            meta=response.meta,
            dont_filter=True,
        )

    def parse_retrieved_page(self, response):
        yield scrapy.FormRequest.from_response(
            response,
            formid="Form1",
            formdata={
                "ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$DownloadDDL": "EXCEL",
            },
            clickdata={
                "name": "ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload"
            },
            callback=self.parse_export,
            meta=response.meta,
            dont_filter=True,
        )

    def parse_export(self, response):
        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        content_type = (
            response.headers.get(b"Content-Type", b"")
            .decode("utf-8", errors="ignore")
            .lower()
        )

        rows: list[list[str]] = []
        body = response.body

        if body[:2] == b"PK":
            rows = _xlsx_rows(body)
        elif "html" in content_type or body.lstrip().startswith(b"<"):
            table_rows = response.xpath("//table//tr")
            for tr in table_rows:
                values = [
                    _clean_cell(text)
                    for text in tr.xpath("./th//text() | ./td//text()").getall()
                ]
                values = [value for value in values if value]
                if values:
                    rows.append(values)
        else:
            self.logger.warning(
                "Unsupported El Paso capacity export format content_type=%s url=%s",
                content_type,
                response.url,
            )
            yield {
                "error": "unsupported_export",
                "content_type": content_type,
                "url": response.url,
            }
            return

        if not rows:
            self.logger.warning("No rows found in El Paso capacity export")
            return

        headers = [_normalize_header(value) for value in rows[0]]

        for row in rows[1:]:
            padded = row + [""] * max(0, len(headers) - len(row))
            values = {
                header: _clean_cell(value)
                for header, value in zip(headers, padded)
                if header
            }

            if not values:
                continue

            item = CapacityItem()
            item["source_url"] = response.url
            item["downloaded_at_utc"] = downloaded_at_utc
            item["requested_post_date_utc"] = str(response.meta["target_date"])

            for raw_key, raw_value in values.items():
                item_key = HEADER_MAP.get(raw_key)
                if item_key and raw_value != "":
                    item[item_key] = raw_value

            if "TSP" not in item:
                item["TSP"] = "37400"
            if "TSP_Name" not in item:
                item["TSP_Name"] = "EL PASO NATURAL GAS COMPANY, LLC"

            if len(item) > 2:
                yield item
