import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

import scrapy
from gas_ebb.items import NoticeItem

FORMAT_DATE_TIME_STRING = "%m/%d/%Y %I:%M:%S%p"
FORMAT_DATE_STRING = "%m/%d/%Y"


class ElPasoNoticesSpider(scrapy.Spider):
    name = "elpaso_notices"
    start_urls = [
        "https://pipeline2.kindermorgan.com/Notices/Notices.aspx?type=C&code=EPGD"
    ]
    allowed_domains = ["kindermorgan.com"]

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
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True,
            )

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True,
            )

    def fix_dt_spacing(self, input):
        input = re.sub(" +", " ", input)
        input = input.lower()
        input = input.replace(" pm", "pm")
        input = input.replace(" am", "am")
        return input

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
            values = texts[1:]
            fields[label] = values

        return fields

    def parse(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)

        for row in response.xpath(
            "//table/tbody[contains(@class, 'igg_NautilusFixedColumnCellCssClass')]/tr"
        ):
            columns = row.xpath("td/div/table/tbody/tr/td/text()")
            posted_raw = columns[2].get() if len(columns) > 2 else None
            posted_date_dt = self.parse_dt(posted_raw, FORMAT_DATE_TIME_STRING)
            if not posted_date_dt:
                continue

            if posted_date_dt.date() < cutoff_date:
                break

            notice_id = columns[5].get() if len(columns) > 5 else None
            if not notice_id:
                continue

            params = {"code": "EPGD", "notc_nbr": notice_id}
            url = f"https://pipeline2.kindermorgan.com/Notices/NoticeDetail.aspx?{urlencode(params)}"
            yield scrapy.Request(
                url=url,
                callback=self.parse_detail,
                dont_filter=True,
            )

    def parse_detail(self, response):
        notice = NoticeItem()
        notice["kind"] = "pipeline"
        notice["url"] = response.url
        notice["name"] = "El Paso Natural Gas"
        notice["tsp"] = "EPGD"

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

        notice["body"] = response.xpath(
            "//div[contains(@id, 'WebSplitter1_tmpl1_ContentPlaceHolder1_Panel1')]/div/table/descendant::div[contains(@class, 'WordSection1')]"
        ).get()

        if not notice.get("body"):
            notice["body"] = response.xpath(
                "//div[contains(@id, 'WebSplitter1_tmpl1_ContentPlaceHolder1_Panel1')]"
            ).get()

        yield notice
