import csv
import io
from datetime import datetime, timedelta, timezone

import scrapy
from gas_ebb.items import CapacityItem

SO_CAL_NAME = "Southern California Gas Company"
SO_CAL_TSP = "SoCalGas"

AVAIL_SCHED_URL = "https://www.socalgasenvoy.com/external/availschedcap/availSchedCap.csv"
UNSUBSCRIBED_URL = (
    "https://www.socalgasenvoy.com/external/contractedcapreport/contractedCapReport.csv"
)


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def _parse_mmddyyyy(value):
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    try:
        return datetime.strptime(cleaned, "%m/%d/%Y").date()
    except ValueError:
        return None


class SoCalCapacitySpider(scrapy.Spider):
    name = "so_cal_capacity"
    allowed_domains = ["socalgasenvoy.com"]
    mongo_collection = "ebb_so_cal_capacity"
    mongo_unique_fields = ["Loc_Name", "Eff_Gas_Day", "Cycle_Desc", "Cap_Type_Desc"]

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
        yield scrapy.Request(
            AVAIL_SCHED_URL,
            callback=self.parse_avail_sched,
            dont_filter=True,
        )
        yield scrapy.Request(
            UNSUBSCRIBED_URL,
            callback=self.parse_unsubscribed,
            dont_filter=True,
        )

    def _base_item(self, response):
        item = CapacityItem()
        item["source_url"] = response.url
        item["downloaded_at_utc"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        item["requested_post_date_utc"] = datetime.now(timezone.utc).date().isoformat()
        item["TSP"] = SO_CAL_TSP
        item["TSP_Name"] = SO_CAL_NAME
        item["Loc_Purp_Desc"] = "Receipt Point"
        return item

    def _within_window(self, value):
        target_date = _parse_mmddyyyy(value)
        if not target_date:
            return True
        cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=self.days_ago)
        return target_date >= cutoff_date

    def parse_avail_sched(self, response):
        text = response.text
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            flow_date = row.get("Gas Flow Date")
            if not self._within_window(flow_date):
                continue

            item = self._base_item(response)
            item["Cap_Type_Desc"] = "Available vs Scheduled"
            item["Post_Date"] = _clean_text(flow_date)
            item["Eff_Gas_Day"] = _clean_text(flow_date)
            item["Cycle_Desc"] = _clean_text(row.get("Cycle"))
            item["Loc_Name"] = _clean_text(row.get("Receipt Point"))
            item["Operating_Capacity"] = _clean_text(
                row.get("Gross Operating Capacity (Dth)")
            )
            item["Total_Nomination"] = _clean_text(row.get("Total Nomination (Dth)"))
            item["Total_Scheduled_Quantity"] = _clean_text(
                row.get("Total Scheduled (Dth)")
            )
            item["Firm_Nomination_Primary"] = _clean_text(
                row.get("Firm Nomination Primary (Dth)")
            )
            item["Firm_Scheduled_Primary"] = _clean_text(
                row.get("Firm Scheduled Primary (Dth)")
            )
            item["Firm_Nomination_Within_Zone"] = _clean_text(
                row.get("Firm Nomination Within Zone (Dth)")
            )
            item["Firm_Scheduled_Within_Zone"] = _clean_text(
                row.get("Firm Scheduled Within Zone (Dth)")
            )
            item["Firm_Nomination_Outside_Zone"] = _clean_text(
                row.get("Firm Nomination Outside Zone (Dth)")
            )
            item["Firm_Scheduled_Outside_Zone"] = _clean_text(
                row.get("Firm Scheduled Outside Zone (Dth)")
            )
            item["Interruptible_Nomination"] = _clean_text(
                row.get("Interruptible Nomination (Dth)")
            )
            item["Interruptible_Scheduled"] = _clean_text(
                row.get("Interruptible Scheduled (Dth)")
            )

            if item.get("Loc_Name"):
                yield item

    def parse_unsubscribed(self, response):
        text = response.text
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            flow_date = row.get("Flow Date")
            if not self._within_window(flow_date):
                continue

            item = self._base_item(response)
            item["Cap_Type_Desc"] = "Unsubscribed"
            item["Post_Date"] = _clean_text(flow_date)
            item["Eff_Gas_Day"] = _clean_text(flow_date)
            item["Loc_Name"] = _clean_text(row.get("Receipt Point"))
            item["Total_Design_Capacity"] = _clean_text(row.get("Total Firm Capacity"))
            item["Contracted_Firm_Rights"] = _clean_text(
                row.get("Contracted Firm Rights")
            )
            item["Operationally_Available_Capacity"] = _clean_text(
                row.get("Available Firm Capacity")
            )

            if item.get("Loc_Name"):
                yield item
