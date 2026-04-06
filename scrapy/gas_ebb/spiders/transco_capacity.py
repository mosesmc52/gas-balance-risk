import csv
import io
from datetime import datetime, timedelta, timezone

import scrapy
from gas_ebb.items import CapacityItem

TRANSCO_CYCLE_CODES = {
    "1": "Timely",
    "2": "Evening",
    "3": "ID1",
    "4": "ID2",
    "8": "ID3",
    "5": "Post",
    "7": "Retro",
}

TRANSCO_CYCLE_ALIASES = {
    "1": "1",
    "timely": "1",
    "2": "2",
    "evening": "2",
    "3": "3",
    "id1": "3",
    "intraday1": "3",
    "intraday_1": "3",
    "4": "4",
    "id2": "4",
    "intraday2": "4",
    "intraday_2": "4",
    "8": "8",
    "id3": "8",
    "intraday3": "8",
    "intraday_3": "8",
    "5": "5",
    "post": "5",
    "7": "7",
    "retro": "7",
}

CSV_FIELD_MAP = {
    "TSP": "TSP",
    "TSP Name": "TSP_Name",
    "Effective Gas Day": "Eff_Gas_Day",
    "Effective Time": "Eff_Time",
    "Cycle Desc": "Cycle_Desc",
    "Posting Date": "Post_Date",
    "Posting Time": "Post_Time",
    "Meas Basis Desc": "Meas_Basis_Desc",
    "Loc": "Loc",
    "Loc Purp Desc": "Loc_Purp_Desc",
    "Loc Purp  Desc": "Loc_Purp_Desc",
    "Flow Ind": "Flow_Ind_Desc",
    "Loc QTI": "Loc_QTI_Desc",
    "Loc Name": "Loc_Name",
    "Loc Zn": "Loc_Zn",
    "Design Capacity": "Total_Design_Capacity",
    "Operating Capacity": "Operating_Capacity",
    "Total Scheduled Quantity": "Total_Scheduled_Quantity",
    "Operationally Available Capacity": "Operationally_Available_Capacity",
    "IT Indicator": "IT",
    "All Qty Avail": "All_Qty_Avail",
    "Qty Reason": "Qty_Reason",
}


class TranscoCapacitySpider(scrapy.Spider):
    name = "transco_capacity"
    allowed_domains = ["1line.williams.com", "www.1line.williams.com"]
    start_url = "https://www.1line.williams.com/ebbCode/OACQueryRequest.jsp?BUID=80&type=OAC"
    csv_url = "https://www.1line.williams.com/ebbCode/OACreportCSV.jsp"
    mongo_collection = "ebb_transco_capacity"
    mongo_unique_fields = ["Loc", "Post_Date", "Post_Time", "Cycle_Desc", "TSP"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        raw_days_ago = kwargs.get("days_ago", "1")
        try:
            self.days_ago = max(1, int(raw_days_ago))
        except ValueError:
            self.days_ago = 1

        self.location_ids = (kwargs.get("location_ids") or "").strip()
        self.cycle_codes = self._parse_cycles(kwargs.get("cycles"))

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
            for cycle_code in self.cycle_codes:
                yield scrapy.Request(
                    self.start_url,
                    callback=self.parse_query_page,
                    meta={
                        "target_date": target_date,
                        "cycle_code": cycle_code,
                        "cookiejar": cookiejar_id,
                    },
                    dont_filter=True,
                )
                cookiejar_id += 1

    def _parse_cycles(self, raw_cycles):
        if not raw_cycles:
            return list(TRANSCO_CYCLE_CODES.keys())

        cycle_codes = []
        for part in str(raw_cycles).split(","):
            key = part.strip().lower()
            if not key:
                continue
            cycle_code = TRANSCO_CYCLE_ALIASES.get(key)
            if cycle_code and cycle_code not in cycle_codes:
                cycle_codes.append(cycle_code)

        if cycle_codes:
            return cycle_codes

        self.logger.warning(
            "No valid cycles found in %r; defaulting to all Transco cycles", raw_cycles
        )
        return list(TRANSCO_CYCLE_CODES.keys())

    def parse_query_page(self, response):
        target_date = response.meta["target_date"]
        cycle_code = response.meta["cycle_code"]

        target_date_str = target_date.strftime("%m/%d/%Y")

        yield scrapy.FormRequest.from_response(
            response,
            formname="myform",
            formdata={
                "MapID": "0",
                "submitflag": "true",
                "tbGasFlowBeginDate": target_date_str,
                "tbGasFlowEndDate": target_date_str,
                "cycle": cycle_code,
                "locationIDs": self.location_ids,
                "reportType": "OAC",
            },
            callback=self.parse_query_result,
            meta=response.meta,
            dont_filter=True,
        )

    def parse_query_result(self, response):
        record_count = response.css('input[name="recordCount"]::attr(value)').get()
        record_limit = response.css('input[name="recordLimit"]::attr(value)').get()

        if record_count and record_limit:
            try:
                if int(record_count) > int(record_limit):
                    self.logger.warning(
                        "Transco OAC result exceeds limit for %s cycle=%s record_count=%s limit=%s",
                        response.meta["target_date"],
                        TRANSCO_CYCLE_CODES.get(
                            response.meta["cycle_code"], response.meta["cycle_code"]
                        ),
                        record_count,
                        record_limit,
                    )
                    return
            except ValueError:
                self.logger.warning(
                    "Unexpected record count values record_count=%r record_limit=%r",
                    record_count,
                    record_limit,
                )

        yield scrapy.Request(
            self.csv_url,
            callback=self.parse_csv,
            meta=response.meta,
            dont_filter=True,
        )

    def parse_csv(self, response):
        content_type = (
            response.headers.get(b"Content-Type", b"")
            .decode("utf-8", errors="ignore")
            .lower()
        )
        disposition = response.headers.get(b"Content-Disposition", b"").decode(
            "utf-8", errors="ignore"
        )

        if "csv" not in content_type and "attachment" not in disposition.lower():
            self.logger.warning(
                "Did not receive Transco OAC CSV. Content-Type=%s Disposition=%s url=%s",
                content_type,
                disposition,
                response.url,
            )
            yield {
                "error": "not_csv",
                "content_type": content_type,
                "disposition": disposition,
                "url": response.url,
                "target_date": str(response.meta.get("target_date")),
                "cycle_code": response.meta.get("cycle_code"),
            }
            return

        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        csv_text = response.body.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(csv_text))

        for row in reader:
            cleaned_row = {}
            for key, value in row.items():
                if key is None:
                    continue
                normalized_key = " ".join(key.split())
                cleaned_row[normalized_key] = (
                    value.strip() if isinstance(value, str) else value
                )

            item = CapacityItem()
            item["source_url"] = response.url
            item["downloaded_at_utc"] = downloaded_at_utc
            item["requested_post_date_utc"] = str(response.meta["target_date"])

            for raw_key, item_key in CSV_FIELD_MAP.items():
                if raw_key in cleaned_row and cleaned_row[raw_key] != "":
                    item[item_key] = cleaned_row[raw_key]

            if "Cycle_Desc" not in item or not item["Cycle_Desc"]:
                item["Cycle_Desc"] = TRANSCO_CYCLE_CODES.get(
                    response.meta["cycle_code"], response.meta["cycle_code"]
                )

            yield item
