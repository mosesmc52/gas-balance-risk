from datetime import datetime, timedelta, timezone

import scrapy
from gas_ebb.items import CapacityItem

CAPACITY_FIELD_MAP = {
    "cycle desc": "Cycle_Desc",
    "cycle": "Cycle_Desc",
    "post date": "Post_Date",
    "posting date": "Post_Date",
    "eff gas day": "Eff_Gas_Day",
    "effective gas day": "Eff_Gas_Day",
    "cap type desc": "Cap_Type_Desc",
    "capacity type": "Cap_Type_Desc",
    "post time": "Post_Time",
    "posting time": "Post_Time",
    "eff time": "Eff_Time",
    "effective time": "Eff_Time",
    "loc": "Loc",
    "location": "Loc",
    "loc name": "Loc_Name",
    "location name": "Loc_Name",
    "loc zn": "Loc_Zn",
    "zone": "Loc_Zn",
    "flow ind desc": "Flow_Ind_Desc",
    "flow direction": "Flow_Ind_Desc",
    "loc purp desc": "Loc_Purp_Desc",
    "loc qti desc": "Loc_QTI_Desc",
    "meas basis desc": "Meas_Basis_Desc",
    "it": "IT",
    "all qty avail": "All_Qty_Avail",
    "total design capacity": "Total_Design_Capacity",
    "design capacity": "Total_Design_Capacity",
    "operating capacity": "Operating_Capacity",
    "total scheduled quantity": "Total_Scheduled_Quantity",
    "scheduled quantity": "Total_Scheduled_Quantity",
    "operationally available capacity": "Operationally_Available_Capacity",
    "available quantity": "Operationally_Available_Capacity",
    "tsp name": "TSP_Name",
    "tsp": "TSP",
    "qty reason": "Qty_Reason",
}


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


class ANRCapacitySpider(scrapy.Spider):
    name = "anr_capacity"
    allowed_domains = ["ebb.tceconnects.com"]
    custom_settings = {"DOWNLOAD_TIMEOUT": 120}
    mongo_collection = "ebb_anr_capacity"
    mongo_unique_fields = ["Loc", "Post_Date", "Post_Time", "Cycle_Desc", "Cap_Type_Desc"]

    report_urls = {
        "Operationally Available": (
            "https://ebb.tceconnects.com/infopost/ReportViewer.aspx"
            "?/InfoPost/OperationallyAvailableCapacity&pAssetNbr=3005"
        ),
        "Unsubscribed": (
            "https://ebb.tceconnects.com/infopost/ReportViewer.aspx"
            "?%2fInfoPost%2fUnsubscribedCapacity&AssetNbr=3005"
        ),
    }
    valid_capacity_fields = set(CapacityItem.fields.keys())

    def __init__(self, days_ago=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.days_ago = int(kwargs.get("days_ago", days_ago))
        except (TypeError, ValueError):
            self.days_ago = 1
        if self.days_ago < 0:
            self.days_ago = 0

    async def start(self):
        for request in self._build_initial_requests():
            yield request

    def start_requests(self):
        yield from self._build_initial_requests()

    def _build_initial_requests(self):
        for report_type, url in self.report_urls.items():
            yield scrapy.Request(
                url=url,
                callback=self.parse_capacity_report,
                meta={"report_type": report_type},
                dont_filter=True,
            )

    def _map_field_name(self, key):
        cleaned = _clean_text(key).lower()
        return CAPACITY_FIELD_MAP.get(cleaned, key)

    def _yield_row_item(self, row, report_type, response_url, downloaded_at_utc):
        item = CapacityItem()
        item["source_url"] = response_url
        item["downloaded_at_utc"] = downloaded_at_utc
        item["requested_post_date_utc"] = datetime.now(timezone.utc).date().isoformat()
        item["TSP"] = "ANR"
        item["TSP_Name"] = "ANR Pipeline Company"

        saw_cap_type = False
        for key, value in row.items():
            mapped_key = self._map_field_name(key)
            if mapped_key not in self.valid_capacity_fields:
                continue
            cleaned_value = _clean_text(value)
            item[mapped_key] = cleaned_value
            if mapped_key == "Cap_Type_Desc" and cleaned_value:
                saw_cap_type = True

        if not saw_cap_type:
            item["Cap_Type_Desc"] = report_type

        post_date = _clean_text(item.get("Post_Date"))
        if post_date and self.days_ago >= 0:
            try:
                post_dt = datetime.strptime(post_date, "%m/%d/%Y")
                cutoff = datetime.now().date() - timedelta(days=self.days_ago)
                if post_dt.date() < cutoff:
                    return
            except ValueError:
                pass

        yield item

    def parse_capacity_report(self, response):
        report_type = response.meta["report_type"]
        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        yielded = False
        for table in response.xpath("//table[.//tr]"):
            header_cells = table.xpath(".//tr[1]/*[self::th or self::td]//text()").getall()
            headers = [_clean_text(x) for x in header_cells if _clean_text(x)]
            if len(headers) < 2:
                continue

            mapped_headers = [self._map_field_name(h) for h in headers]
            if not any(h in self.valid_capacity_fields for h in mapped_headers):
                continue

            for row in table.xpath(".//tr[position() > 1]"):
                values = [
                    _clean_text(x)
                    for x in row.xpath("./td//text()[normalize-space()]").getall()
                ]
                if not values or len(values) < 2:
                    continue

                row_dict = dict(zip(headers[: len(values)], values))
                for item in self._yield_row_item(
                    row=row_dict,
                    report_type=report_type,
                    response_url=response.url,
                    downloaded_at_utc=downloaded_at_utc,
                ):
                    yielded = True
                    yield item

        if not yielded:
            self.logger.warning(
                "ANR capacity report did not yield parseable table rows for %s: %r",
                report_type,
                response.text[:200],
            )
