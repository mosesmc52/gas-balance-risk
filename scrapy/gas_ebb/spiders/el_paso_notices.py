import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

import scrapy
from gas_ebb.items import NoticeItem
from scrapy_splash import SplashRequest

FORMAT_DATE_TIME_STRING = "%m/%d/%Y %I:%M:%S%p"
FORMAT_DATE_STRING = "%m/%d/%Y"


class ElPasoSpider(scrapy.Spider):
    name = "elpaso"
    start_urls = [
        "https://pipeline2.kindermorgan.com/Notices/Notices.aspx?type=C&code=EPGD"
    ]

    allowed_domains = ["kindermorgan.com"]

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url,
                self.parse,
            )

    def fix_dt_spacing(self, input):
        input = re.sub(" +", " ", input)
        input = input.lower()
        input = input.replace(" pm", "pm")
        input = input.replace(" am", "am")

        return input

    def parse(self, response):
        for row in response.xpath(
            "//table/tbody[contains(@class, 'igg_NautilusFixedColumnCellCssClass')]/tr"
        ):
            colums = row.xpath("td/div/table/tbody/tr/td/text()")

            posted_date_dt = datetime.strptime(colums[2].get(), FORMAT_DATE_TIME_STRING)

            is_posted_date_after_a_day_before_current_date = (
                posted_date_dt.date() >= datetime.now().date() - timedelta(days=1)
            )

            if not is_posted_date_after_a_day_before_current_date:
                break

            id = colums[5].get()

            params = {"code": "EPGD", "notc_nbr": id}
            url = f"https://pipeline2.kindermorgan.com/Notices/NoticeDetail.aspx?{urlencode(params)}"
            yield SplashRequest(
                url,
                self.parse_detail,
            )

    def parse_detail(self, response):
        # Example: https://pipeline2.kindermorgan.com/Notices/NoticeDetail.aspx?code=TGP&notc_nbr=388379
        notice = crawlers.items.Notice()
        notice["kind"] = "pipeline"
        notice["url"] = response.url
        notice["name"] = "Tennessee"

        rows = response.xpath(
            "//div[contains(@id, 'WebSplitter1_tmpl1_ContentPlaceHolder1_Panel1')]/table/tbody/tr"
        )

        # name
        # rows[0].xpath("./td/span[position() = 2]/text()")[0].get()

        notice["critical"] = rows[0].xpath("./td/span[position() = 2]/text()")[1].get()

        # notice["type"] = rows[1].xpath('./td/span[position() = 2]/text()')[0].get()
        notice["type"] = rows[1].xpath("./td/span[position() = 2]/text()")[1].get()

        notice["effective_dt"] = datetime.strptime(
            self.fix_dt_spacing(
                rows[2].xpath("./td/span[position() = 2]/text()")[0].get()
            ),
            FORMAT_DATE_TIME_STRING,
        )

        notice["end_dt"] = datetime.strptime(
            self.fix_dt_spacing(
                rows[2].xpath("./td/span[position() = 2]/text()")[1].get()
            ),
            FORMAT_DATE_TIME_STRING,
        )
        notice["posted_dt"] = datetime.strptime(
            self.fix_dt_spacing(
                rows[3].xpath("./td/span[position() = 2]/text()")[0].get()
            ),
            FORMAT_DATE_TIME_STRING,
        )
        notice["notice_id"] = rows[2].xpath("./td/span[position() = 2]/text()")[1].get()
        notice["response_dt"] = datetime.strptime(
            self.fix_dt_spacing(
                rows[4].xpath("./td/span[position() = 2]/text()")[1].get()
            ),
            FORMAT_DATE_STRING,
        )

        if notice["response_dt"]:
            notice["response"] = "Y"
        else:
            notice["response"] = "N"

        notice["subject"] = rows[6].xpath("./td/span[position() = 2]/text()")[0].get()
        notice["body"] = response.xpath(
            "//div[contains(@id, 'WebSplitter1_tmpl1_ContentPlaceHolder1_Panel1')]/div/table/descendant::div[contains(@class, 'WordSection1')]"
        ).get()

        yield notice
