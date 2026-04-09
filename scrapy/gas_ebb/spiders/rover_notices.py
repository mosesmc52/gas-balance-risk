import csv
import io
from datetime import datetime, timedelta
from urllib.parse import urlencode

import scrapy
from gas_ebb.items import NoticeItem

ROVER_NAME = "Rover Pipeline LLC"
ROVER_TSP = "ROVER"
NOTICE_LIST_DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def _parse_notice_dt(value):
    value = _clean_text(value)
    if not value:
        return None
    try:
        return datetime.strptime(value, NOTICE_LIST_DT_FORMAT)
    except ValueError:
        return None


class RoverNoticesSpider(scrapy.Spider):
    name = "rover_notices"
    allowed_domains = ["rovermessenger.energytransfer.com"]
    mongo_collection = "ebb_rover_notices"
    mongo_unique_fields = ["notice_id"]
    notice_types = {
        "Y": "https://rovermessenger.energytransfer.com/ipost/notice/critical?asset=ROVER&f=csv&extension=csv",
        "N": "https://rovermessenger.energytransfer.com/ipost/notice/non-critical?asset=ROVER&f=csv&extension=csv",
    }

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
        for critical, url in self.notice_types.items():
            yield scrapy.Request(
                url=url,
                callback=self.parse_csv,
                meta={"critical": critical},
                dont_filter=True,
            )

    def parse_csv(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)
        text = response.body.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            posted_dt = _parse_notice_dt(row.get("Posted Date/Time"))
            if posted_dt and posted_dt.date() < cutoff_date:
                break

            notice_id = _clean_text(row.get("Notice ID"))
            if not notice_id:
                continue

            params = {"asset": "ROVER"}
            detail_url = (
                "https://rovermessenger.energytransfer.com/ipost/notice/show/"
                f"{notice_id}?{urlencode(params)}"
            )
            yield scrapy.Request(
                detail_url,
                callback=self.parse_detail,
                meta={
                    "critical": response.meta.get("critical"),
                    "posted_dt": posted_dt,
                    "effective_dt": _parse_notice_dt(row.get("Notice Eff Date/Time")),
                    "subject": _clean_text(row.get("Subject")),
                    "list_type": _clean_text(row.get("Notice Type")),
                    "notice_id": notice_id,
                },
                dont_filter=True,
            )

    def _extract_detail_fields(self, response):
        fields = {}
        for row in response.xpath("//table[contains(@class, 'table')]//tr"):
            label = _clean_text(row.xpath("./td[1]//text()").get())
            if not label:
                continue
            label = label.rstrip(":").lower()
            value_text = _clean_text(" ".join(row.xpath("./td[2]//text()").getall()))
            value_html = row.xpath("./td[2]").get()
            fields[label] = {
                "text": value_text,
                "html": value_html,
            }
        return fields

    def parse_detail(self, response):
        notice = NoticeItem()
        notice["kind"] = "pipeline"
        notice["url"] = response.url
        notice["name"] = ROVER_NAME
        notice["service_provider"] = ROVER_NAME
        notice["tsp"] = ROVER_TSP

        fields = self._extract_detail_fields(response)

        notice["notice_id"] = fields.get("notice id", {}).get("text") or response.meta.get(
            "notice_id"
        )
        notice["type"] = fields.get("notice type", {}).get("text") or response.meta.get(
            "list_type"
        )
        notice["critical"] = fields.get("critical", {}).get("text") or response.meta.get(
            "critical"
        )
        notice["status"] = fields.get("notice status description", {}).get("text")
        notice["subject"] = fields.get("subject", {}).get("text") or response.meta.get(
            "subject"
        )

        posted_dt = _parse_notice_dt(fields.get("posting date/time", {}).get("text"))
        notice["posted_dt"] = posted_dt or response.meta.get("posted_dt")

        effective_dt = _parse_notice_dt(
            fields.get("notice effective date/time", {}).get("text")
        )
        if effective_dt:
            notice["effective_dt"] = effective_dt
        elif response.meta.get("effective_dt"):
            notice["effective_dt"] = response.meta["effective_dt"]

        end_dt = _parse_notice_dt(fields.get("notice end date/time", {}).get("text"))
        if end_dt and end_dt.year < 2200:
            notice["end_dt"] = end_dt

        response_desc = fields.get("reqrd rsp desc", {}).get("text", "")
        if response_desc and response_desc.lower() != "no response required":
            notice["response"] = "Y"
        else:
            notice["response"] = "N"

        body_html = fields.get("notice text", {}).get("html") or ""
        notice["body"] = body_html

        yield notice
