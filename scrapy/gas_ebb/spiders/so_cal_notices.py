import html
import re
from datetime import datetime, timedelta

import scrapy
from gas_ebb.items import NoticeItem

SO_CAL_NAME = "Southern California Gas Company"
SO_CAL_TSP = "SoCalGas"
DATE_FORMAT = "%m/%d/%Y %I:%M %p"
NOTICE_ROW_RE = re.compile(
    r'\{\s*"Message Id"\s*:\s*"(?P<message_id>[^"]+)"\s*,\s*'
    r'"Message Subject"\s*:\s*"(?P<subject>(?:[^"\\]|\\.)*)"\s*,\s*'
    r'"Category"\s*:\s*\'(?P<category>(?:[^\'\\]|\\.)*)\'\s*,\s*'
    r'"Date Posted"\s*:\s*"(?P<posted>(?:[^"\\]|\\.)*)"\s*,\s*'
    r'"Attachment"\s*:\s*\'(?P<attachment>(?:[^\'\\]|\\.)*)\'\s*'
    r"\}",
    re.S,
)


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def _decode_js_string(value):
    cleaned = (value or "").replace(r"\/", "/").replace(r"\'", "'").replace(r"\"", '"')
    return html.unescape(cleaned)


def _parse_posted_dt(value):
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    cleaned = re.sub(r"\s+[A-Z]{2,4}$", "", cleaned)
    try:
        return datetime.strptime(cleaned, DATE_FORMAT)
    except ValueError:
        return None


class SoCalNoticesSpider(scrapy.Spider):
    name = "so_cal_notices"
    allowed_domains = ["socalgasenvoy.com"]
    mongo_collection = "ebb_so_cal_notices"
    mongo_unique_fields = ["notice_id"]

    folder_urls = {
        "Y": "https://www.socalgasenvoy.com/Public/ViewExternalEbb.getMessageLedger?folderId=1",
        "N": "https://www.socalgasenvoy.com/Public/ViewExternalEbb.getMessageLedger?folderId=2",
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
        for critical, url in self.folder_urls.items():
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"critical": critical},
                dont_filter=True,
            )

    def parse(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)
        matches = NOTICE_ROW_RE.finditer(response.text)

        saw_rows = False
        for match in matches:
            saw_rows = True
            posted_dt = _parse_posted_dt(match.group("posted"))
            if posted_dt and posted_dt.date() < cutoff_date:
                break

            raw_message_id = _decode_js_string(match.group("message_id"))
            notice_id = raw_message_id.split("~", 1)[0]
            subject = _clean_text(_decode_js_string(match.group("subject")))
            category = _clean_text(_decode_js_string(match.group("category")))
            attachment = _decode_js_string(match.group("attachment"))

            notice = NoticeItem()
            notice["kind"] = "pipeline"
            notice["name"] = "SoCalGas"
            notice["tsp"] = SO_CAL_TSP
            notice["service_provider"] = SO_CAL_NAME
            notice["url"] = response.url
            notice["notice_id"] = notice_id
            notice["subject"] = subject
            notice["type"] = category
            notice["critical"] = response.meta["critical"]
            notice["posted_dt"] = posted_dt
            notice["response"] = "N"

            if "Critical" in category:
                notice["critical"] = "Y"
            elif "Non-Critical" in category:
                notice["critical"] = "N"

            if attachment and attachment != "&nbsp;":
                notice["file"] = "Y"

            yield notice

        if not saw_rows:
            self.logger.warning("No SoCal notice rows found at %s", response.url)
