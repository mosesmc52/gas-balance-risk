from datetime import datetime, timedelta
from urllib.parse import urlencode

import scrapy
from gas_ebb.items import NoticeItem
from scrapy_splash import SplashRequest

FORMAT_DATE_TIME_STRING = "%m/%d/%Y %I:%M:%S %p"


class TexasEasternSpider(scrapy.Spider):
    # https://infopost.enbridge.com/infopost/TEHome.asp?Pipe=TE
    name = "texaseastern"
    allowed_domains = ["infopost.enbridge.com"]
    start_urls = ["https://infopost.enbridge.com/infopost/TEHome.asp?Pipe=TE"]

    def start_requests(self):

        for url in self.start_urls:
            yield SplashRequest(
                url,
                self.next_requests,
            )

    def next_requests(self, response):
        for url in [
            "https://infopost.enbridge.com/infopost/NoticesList.asp?pipe=TE&type=CRI",
            "https://infopost.enbridge.com/infopost/NoticesList.asp?pipe=TE&type=NON",
        ]:
            yield SplashRequest(
                url,
                self.parse,
            )

    def parse(self, response):

        now = datetime.now()
        for row in response.xpath(
            "//table/tbody/tr[contains(@class, 'even') or contains(@class, 'odd')]"
        ):
            url = f'https://infopost.enbridge.com/infopost/{ row.xpath("td[last()-1]/a/@href").extract()[0] }'

            posted_date_utc_dt = datetime.strptime(
                row.xpath("td[2]/text()").extract()[0], FORMAT_DATE_TIME_STRING
            )
            is_posted_date_after_a_day_before_current_date = (
                posted_date_utc_dt.date() >= datetime.now().date() - timedelta(days=1)
            )
            if not is_posted_date_after_a_day_before_current_date:
                break

            yield SplashRequest(
                url,
                self.parse_detail,
            )

    def parse_detail(self, response):

        notice = crawlers.items.Notice()
        notice["kind"] = "pipeline"
        notice["url"] = response.url

        data = response.xpath('//div[contains(@id, "headingData")]/text()').extract()
        notice["tsp"] = data[0]
        notice["name"] = data[1]
        notice["notice_id"] = data[7]
        notice["critical"] = data[2]
        notice["effective_dt"] = datetime.strptime(
            f"{data[3]} {data[4]}", FORMAT_DATE_TIME_STRING
        )
        notice["end_dt"] = datetime.strptime(
            f"{data[5]} {data[6]}", FORMAT_DATE_TIME_STRING
        )
        notice["status"] = data[8]
        notice["type"] = data[9]
        notice["posted_dt"] = datetime.strptime(
            f"{data[10]} {data[11]}", FORMAT_DATE_TIME_STRING
        )
        notice["prior_id"] = data[12].strip()
        if "No" in data[13]:
            notice["response"] = "No"
        else:
            notice["response"] = "Yes"
            notice["response_dt"] = datetime.strptime(
                f"{data[14]} {data[15]}", FORMAT_DATE_TIME_STRING
            )
        notice["subject"] = data[16]

        notice_text = ""
        for text in response.xpath('//div[contains(@id, "bulletin")]').extract():
            notice_text += text

        notice["body"] = notice_text

        yield notice
