import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

import scrapy
from gas_ebb.items import NoticeItem

FORMAT_DATE_TIME_STRING = "%m/%d/%Y %I:%M:%S%p"
FORMAT_DATE_STRING = "%m/%d/%Y"


class NaturalGasAmericaNoticesSpider(scrapy.Spider):
    name = "natural_gas_america_notices"
    allowed_domains = ["kindermorgan.com"]
    mongo_collection = "ebb_natural_gas_america_notices"
    mongo_unique_fields = ["notice_id"]
    notice_types = {
        "Y": "https://pipeline2.kindermorgan.com/Notices/Notices.aspx?type=C&code=NGPL",
        "N": "https://pipeline2.kindermorgan.com/Notices/Notices.aspx?type=N&code=NGPL",
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
                callback=self.parse,
                meta={"critical": critical},
                dont_filter=True,
            )

    def fix_dt_spacing(self, value):
        value = re.sub(" +", " ", value or "")
        value = value.lower()
        value = value.replace(" pm", "pm")
        value = value.replace(" am", "am")
        return value

    def parse_dt(self, value, fmt):
        value = (value or "").strip()
        if not value:
            return None
        try:
            return datetime.strptime(self.fix_dt_spacing(value), fmt)
        except (TypeError, ValueError):
            return None

    def extract_detail_fields(self, response):
        fields = {}
        rows = response.xpath(
            "//div[contains(@id, 'WebSplitter1_tmpl1_ContentPlaceHolder1_Panel1')]/table//tr"
        )

        for row in rows:
            texts = [
                " ".join(t.split())
                for t in row.xpath(".//span/text() | .//td/text()").getall()
            ]
            texts = [t.strip() for t in texts if t and t.strip()]
            if len(texts) < 2:
                continue

            label = texts[0].rstrip(":").strip().lower()
            fields[label] = texts[1:]

        return fields

    def parse(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)

        for row in response.xpath(
            "//table/tbody[contains(@class, 'igg_NautilusFixedColumnCellCssClass')]/tr"
        ):
            columns = [
                text.strip()
                for text in row.xpath("td/div/table/tbody/tr/td//text()").getall()
                if text and text.strip()
            ]
            if len(columns) < 6:
                continue

            posted_date_dt = self.parse_dt(columns[2], FORMAT_DATE_TIME_STRING)
            if posted_date_dt and posted_date_dt.date() < cutoff_date:
                break

            notice_id = columns[5]
            if not notice_id:
                continue

            params = {"code": "NGPL", "notc_nbr": notice_id}
            url = (
                "https://pipeline2.kindermorgan.com/Notices/NoticeDetail.aspx?"
                f"{urlencode(params)}"
            )
            yield scrapy.Request(
                url=url,
                callback=self.parse_detail,
                meta={
                    "critical": response.meta.get("critical"),
                    "posted_dt": posted_date_dt,
                    "subject": columns[6] if len(columns) > 6 else None,
                },
                dont_filter=True,
            )

    def parse_detail(self, response):
        notice = NoticeItem()
        notice["kind"] = "pipeline"
        notice["url"] = response.url
        notice["name"] = "Natural Gas Pipeline Company of America LLC"
        notice["tsp"] = "NGPL"
        notice["critical"] = response.meta.get("critical")
        if response.meta.get("posted_dt"):
            notice["posted_dt"] = response.meta["posted_dt"]

        fields = self.extract_detail_fields(response)

        critical_values = fields.get("critical", [])
        if critical_values:
            notice["critical"] = critical_values[-1]

        type_values = fields.get("notice type", [])
        if type_values:
            notice["type"] = type_values[-1]

        effective_values = fields.get("notice effective date/time", [])
        if effective_values:
            notice["effective_dt"] = self.parse_dt(
                effective_values[0], FORMAT_DATE_TIME_STRING
            )

        end_values = fields.get("notice end date/time", [])
        if end_values:
            notice["end_dt"] = self.parse_dt(end_values[0], FORMAT_DATE_TIME_STRING)

        posted_values = fields.get("post date/time", [])
        if posted_values:
            notice["posted_dt"] = self.parse_dt(
                posted_values[0], FORMAT_DATE_TIME_STRING
            )

        notice_id_values = fields.get("notice id", [])
        if notice_id_values:
            notice["notice_id"] = notice_id_values[0]

        response_values = fields.get("response date", [])
        if response_values:
            notice["response_dt"] = self.parse_dt(response_values[0], FORMAT_DATE_STRING)
        notice["response"] = "Y" if notice.get("response_dt") else "N"

        subject_values = fields.get("subject", [])
        if subject_values:
            notice["subject"] = subject_values[0]
        elif response.meta.get("subject"):
            notice["subject"] = response.meta["subject"]

        notice["body"] = response.xpath(
            "//div[contains(@id, 'WebSplitter1_tmpl1_ContentPlaceHolder1_Panel1')]/div/table/descendant::div[contains(@class, 'WordSection1')]"
        ).get()

        if not notice.get("body"):
            notice["body"] = response.xpath(
                "//div[contains(@id, 'WebSplitter1_tmpl1_ContentPlaceHolder1_Panel1')]"
            ).get()

        yield notice
