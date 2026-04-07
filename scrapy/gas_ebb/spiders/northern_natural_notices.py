import re
from datetime import datetime, timedelta

import scrapy
from gas_ebb.items import NoticeItem

LIST_DT_FORMAT = "%b %d %Y %I:%M %p"
DETAIL_DT_FORMAT = "%m/%d/%Y %I:%M %p"


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


class NorthernNaturalNoticesSpider(scrapy.Spider):
    name = "nngp_notices"
    allowed_domains = ["northernnaturalgas.com"]
    start_urls = [
        "https://www.northernnaturalgas.com/infopostings/Notices/Pages/Critical.aspx",
        "https://www.northernnaturalgas.com/infopostings/Notices/Pages/NonCritical.aspx",
    ]
    mongo_collection = "ebb_northern_natural_notices"
    mongo_unique_fields = ["notice_id"]

    def __init__(self, days_ago=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.days_ago = int(kwargs.get("days_ago", days_ago))
        except (TypeError, ValueError):
            self.days_ago = 1
        if self.days_ago < 0:
            self.days_ago = 0

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def parse_dt(self, value, fmt):
        value = _clean_text(value)
        if not value:
            return None
        try:
            return datetime.strptime(value, fmt)
        except (TypeError, ValueError):
            return None

    def extract_detail_fields(self, response):
        fields = {}
        for row in response.xpath("//table[contains(@class, 'HistoryTable')]//tr"):
            values = [_clean_text(x) for x in row.xpath(".//span/text()").getall()]
            values = [x for x in values if x]
            if len(values) >= 2:
                fields[values[0].rstrip(":").lower()] = values[1:]
            elif values and ":" in values[0]:
                label, value = values[0].split(":", 1)
                fields[label.strip().lower()] = [_clean_text(value)]

        for row in response.xpath("//table[contains(@class, 'HistoryTable2')]//tr"):
            values = [_clean_text(x) for x in row.xpath(".//span/text()").getall()]
            values = [x for x in values if x]
            if len(values) >= 2:
                fields[values[0].rstrip(":").lower()] = values[1:]

        return fields

    def parse(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)

        for row in response.xpath(
            "//table[contains(@class, 'rgMasterTable')]//tr[contains(@class, 'rgRow') or contains(@class, 'rgAltRow')]"
        ):
            cells = [
                _clean_text(" ".join(cell.xpath(".//text()").getall()))
                for cell in row.xpath("./td")
            ]
            if len(cells) < 8:
                continue

            posted_dt = self.parse_dt(cells[2], LIST_DT_FORMAT)
            if posted_dt and posted_dt.date() < cutoff_date:
                continue

            onclick = row.xpath(".//a[contains(@onclick, 'NoticePopup.aspx')]/@onclick").get()
            if not onclick:
                continue

            match = re.search(r"ID=(\d+)", onclick)
            if not match:
                continue

            notice_id = match.group(1)
            download_href = row.xpath(
                ".//a[contains(@href, '/_layouts/DownloadFile.aspx')]/@href"
            ).get()

            detail_url = (
                "https://www.northernnaturalgas.com/_layouts/15/NoticePopup.aspx"
                f"?ID={notice_id}"
            )

            yield scrapy.Request(
                url=detail_url,
                callback=self.parse_detail,
                meta={
                    "row_values": cells,
                    "download_url": response.urljoin(download_href) if download_href else None,
                },
                dont_filter=True,
            )

    def parse_detail(self, response):
        fields = self.extract_detail_fields(response)
        row_values = response.meta.get("row_values", [])

        notice = NoticeItem()
        notice["kind"] = "pipeline"
        notice["url"] = response.url
        notice["name"] = "Northern Natural Gas Company"
        notice["tsp"] = "784158214"

        if row_values:
            notice["type"] = row_values[1]
            notice["posted_dt"] = self.parse_dt(row_values[2], LIST_DT_FORMAT)
            notice["effective_dt"] = self.parse_dt(row_values[3], LIST_DT_FORMAT)
            notice["end_dt"] = self.parse_dt(row_values[4], LIST_DT_FORMAT)
            notice["notice_id"] = row_values[5].lstrip("0") or row_values[5]
            notice["status"] = row_values[6]
            notice["subject"] = row_values[7]

        notice["file"] = response.meta.get("download_url")

        tsp_values = fields.get("tsp", [])
        if tsp_values:
            notice["tsp"] = tsp_values[0]

        type_values = fields.get("notice type", [])
        if type_values:
            notice["type"] = type_values[0]

        subject_values = fields.get("subject", [])
        if subject_values:
            notice["subject"] = subject_values[0]

        critical_values = fields.get("critical", [])
        if critical_values:
            notice["critical"] = critical_values[0]

        reason_values = fields.get("reason", [])
        if reason_values:
            notice["reason"] = reason_values[0]

        location_values = fields.get("location", [])
        if location_values:
            notice["location"] = location_values[0]

        notice_id_values = fields.get("notice id", [])
        if notice_id_values:
            notice["notice_id"] = notice_id_values[0].lstrip("0") or notice_id_values[0]

        status_values = fields.get("notice status", [])
        if status_values:
            notice["status"] = status_values[0]

        posted_values = fields.get("post date/time", [])
        if posted_values:
            notice["posted_dt"] = self.parse_dt(posted_values[0], DETAIL_DT_FORMAT)

        effective_values = fields.get("notice effective date/time", [])
        if effective_values:
            notice["effective_dt"] = self.parse_dt(
                effective_values[0], DETAIL_DT_FORMAT
            )

        end_values = fields.get("notice end date/time", [])
        if end_values:
            notice["end_dt"] = self.parse_dt(end_values[0], DETAIL_DT_FORMAT)

        response_values = fields.get("required response indicator description", [])
        notice["response"] = "N"
        if response_values:
            notice["response"] = (
                "N" if "no response required" in response_values[0].lower() else "Y"
            )

        notice["body"] = response.xpath("//table[contains(@class, 'noticeDetailMain')]").get()

        yield notice
