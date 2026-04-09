import csv
import io
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import scrapy
from gas_ebb.items import CapacityItem

ROVER_NAME = "Rover Pipeline LLC"
ROVER_TSP = "ROVER"
DATE_INPUT_FORMAT = "%m/%d/%Y"
PAGE_DATETIME_FORMAT = "%m/%d/%Y %I:%M%p"

CSV_FIELD_MAP = {
    "Loc": "Loc",
    "Loc Name": "Loc_Name",
    "Loc Purp Desc": "Loc_Purp_Desc",
    "Loc/QTI": "Loc_QTI_Desc",
    "DC": "Total_Design_Capacity",
    "OPC": "Operating_Capacity",
    "TSQ": "Total_Scheduled_Quantity",
    "OAC": "Operationally_Available_Capacity",
    "Loc Zn": "Loc_Zn",
    "IT": "IT",
    "Flow Ind": "Flow_Ind_Desc",
    "All Qty Avail": "All_Qty_Avail",
    "Qty Reason": "Qty_Reason",
}


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def _parse_page_dt(value):
    value = _clean_text(value)
    if not value:
        return None
    try:
        return datetime.strptime(value, PAGE_DATETIME_FORMAT)
    except ValueError:
        return None


class RoverCapacitySpider(scrapy.Spider):
    name = "rover_capacity"
    allowed_domains = ["rovermessenger.energytransfer.com"]
    base_url = (
        "https://rovermessenger.energytransfer.com/ipost/capacity/"
        "operationally-available-by-location"
    )
    mongo_collection = "ebb_rover_capacity"
    mongo_unique_fields = ["Loc", "Post_Date", "Post_Time", "Cycle_Desc", "TSP"]

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
        for offset in range(self.days_ago):
            target_date = today_utc - timedelta(days=offset)
            query = urlencode(
                {
                    "asset": "ROVER",
                    "gasDay": target_date.strftime(DATE_INPUT_FORMAT),
                }
            )
            yield scrapy.Request(
                f"{self.base_url}?{query}",
                callback=self.parse_page,
                meta={"target_date": target_date},
                dont_filter=True,
            )

    def parse_page(self, response):
        target_date = response.meta["target_date"]
        tsp_name = _clean_text(response.xpath("//h2/text()").get())
        cycle_desc = _clean_text(
            response.css('input[name="cycleDesc"]::attr(value)').get()
        )
        post_dt = _parse_page_dt(
            response.xpath(
                'normalize-space(//p[contains(., "Post Date/Time:")])'
            ).re_first(r"Post Date/Time:\s*(.+)")
        )
        eff_dt = _parse_page_dt(
            response.xpath(
                'normalize-space(//p[contains(., "Eff Gas Day/Time:")])'
            ).re_first(r"Eff Gas Day/Time:\s*(.+)")
        )
        meas_basis = _clean_text(
            response.xpath(
                'normalize-space(//p[contains(., "Measurement Basis Description:")])'
            ).re_first(r"Measurement Basis Description:\s*(.+)")
        )

        query = urlencode(
            {
                "asset": "ROVER",
                "gasDay": target_date.strftime(DATE_INPUT_FORMAT),
                "f": "csv",
                "extension": "csv",
            }
        )
        yield scrapy.Request(
            f"{self.base_url}?{query}",
            callback=self.parse_csv,
            meta={
                "target_date": target_date,
                "cycle_desc": cycle_desc,
                "post_dt": post_dt,
                "eff_dt": eff_dt,
                "meas_basis": meas_basis,
                "tsp_name": tsp_name,
            },
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
                "Did not receive Rover capacity CSV. Content-Type=%s Disposition=%s url=%s",
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
            }
            return

        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        csv_text = response.body.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(csv_text))

        post_dt = response.meta.get("post_dt")
        eff_dt = response.meta.get("eff_dt")
        cycle_desc = response.meta.get("cycle_desc") or ""
        meas_basis = response.meta.get("meas_basis") or ""
        tsp_name = response.meta.get("tsp_name") or ROVER_NAME

        for row in reader:
            cleaned_row = {}
            for key, value in row.items():
                if key is None:
                    continue
                cleaned_row[_clean_text(key)] = _clean_text(value)

            item = CapacityItem()
            item["source_url"] = response.url
            item["downloaded_at_utc"] = downloaded_at_utc
            item["requested_post_date_utc"] = str(response.meta["target_date"])
            item["TSP"] = ROVER_TSP
            item["TSP_Name"] = tsp_name

            if cycle_desc:
                item["Cycle_Desc"] = cycle_desc
            if meas_basis:
                item["Meas_Basis_Desc"] = meas_basis
            if post_dt:
                item["Post_Date"] = post_dt.strftime("%m/%d/%Y")
                item["Post_Time"] = post_dt.strftime("%I:%M %p").lstrip("0")
            if eff_dt:
                item["Eff_Gas_Day"] = eff_dt.strftime("%m/%d/%Y")
                item["Eff_Time"] = eff_dt.strftime("%I:%M %p").lstrip("0")

            for raw_key, item_key in CSV_FIELD_MAP.items():
                value = cleaned_row.get(raw_key)
                if value != "":
                    item[item_key] = value

            if item.get("Loc"):
                yield item
