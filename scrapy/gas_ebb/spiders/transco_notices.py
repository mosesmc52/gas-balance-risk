import json
from datetime import datetime, timedelta

import pytz
import scrapy
from gas_ebb.items import NoticeItem
from scrapy_splash import SplashRequest

FORMAT_TIME_STRING = "%m/%d/%Y %H:%M:%S"

#
# https://www.1line.williams.com/xhtml/notice_list.jsf?buid=80&type=-1&type2=-1&archive=N&critical_ind=N&hfSortField=posted_date&hfSortDir=DESC


class TransCoSpider(scrapy.Spider):
    name = "transco_notices"
    start_urls = [
        "https://www.1line.williams.com/Transco/info-postings/notices/critical-notices.html",
        "https://www.1line.williams.com/Transco/info-postings/notices/non-critical-notices.html",
    ]
    allowed_domains = ["williams.com"]

    def __init__(self, days_ago=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raw_days_ago = kwargs.get("days_ago", days_ago)
        legacy_cutoff_days = kwargs.get("cutoff_days")
        if legacy_cutoff_days is not None and "days_ago" not in kwargs:
            raw_days_ago = legacy_cutoff_days
        try:
            self.days_ago = int(raw_days_ago)
        except (TypeError, ValueError):
            self.days_ago = 1
        if self.days_ago < 0:
            self.days_ago = 0

    def hasPhrase(self, phrases=[], text=""):
        return any([phrase for phrase in phrases if phrase.lower() in text.lower()])

    async def start(self):
        for url in self.start_urls:
            yield SplashRequest(
                url=url,
                callback=self.parse_iframe,
                endpoint="render.json",
                args={"wait": 2.0, "iframes": 1},
                dont_filter=True,
            )

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url=url,
                callback=self.parse_iframe,
                endpoint="render.json",
                args={"wait": 2.0, "iframes": 1},
                dont_filter=True,
            )

    def parse_iframe(self, response):
        json_data = json.loads(response.text)
        iframe_url = json_data["childFrames"][0]["requestedUrl"]

        yield SplashRequest(
            url=iframe_url,
            callback=self.parse_table,
            dont_filter=True,
        )

    def update_to_utc(self, input):
        dt = datetime.strptime(input[:19], FORMAT_TIME_STRING)
        if self.hasPhrase(["CST", "CDT"], input):
            timezone = pytz.timezone("America/Chicago")
        else:
            raise ValueError(f"Timezone '{input[20:]}' not known")

        dt = dt.replace(tzinfo=timezone)

        desired_timezone = pytz.timezone("UTC")
        dt_utc = dt.astimezone(desired_timezone)

        return dt_utc

    def parse_table(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)

        for row in response.xpath("//tbody/tr"):
            effective_date = row.xpath(
                'td[contains(@class,"ui-col-1")]/descendant::span/text()'
            ).get()
            if not effective_date:
                continue

            posted_date_dt = datetime.strptime(effective_date[:19], FORMAT_TIME_STRING)

            if posted_date_dt.date() < cutoff_date:
                break

            notice_path = row.xpath(
                'td[contains(@class,"ui-col-7")]/descendant::a/@href'
            ).get()
            notice_path = notice_path.replace("DownloadFlag=true", "DownloadFlag=false")

            subject = (
                row.xpath('td[contains(@class,"ui-col-5")]/descendant::a/text()')
                .get()
                .strip()
            )

            url = response.urljoin(notice_path)

            yield SplashRequest(
                url=url,
                callback=self.parse_notice,
                meta={"subject": subject},
                dont_filter=True,
            )

    def get_header_element(self, response, position):
        header = response.xpath("//tbody[position() = 1]/tr")
        return (
            header[position].xpath("td[position() = 1]/text()").get().strip(),
            header[position].xpath("td[position() = 2]/text()").get().strip(),
        )

    def parse_notice(self, response):

        notice = NoticeItem()
        notice["kind"] = "pipeline"
        notice["url"] = response.url
        notice["name"] = "Transcontinental"
        notice["subject"] = response.meta.get("subject")

        _, notice["critical"] = self.get_header_element(response, 0)

        _, effective_date = self.get_header_element(response, 1)
        _, effective_time = self.get_header_element(response, 2)
        notice["effective_dt"] = self.update_to_utc(
            f"{effective_date} {effective_time}"
        )

        _, post_date = self.get_header_element(response, 8)
        _, post_time = self.get_header_element(response, 9)
        notice["posted_dt"] = self.update_to_utc(f"{post_date} {post_time}")

        _, end_date = self.get_header_element(response, 3)

        if end_date:
            _, end_time = self.get_header_element(response, 4)
            notice["end_dt"] = self.update_to_utc(f"{end_date} {end_time}")
        else:
            notice["end_dt"] = ""

        _, notice["notice_id"] = self.get_header_element(response, 5)
        _, notice["type"] = self.get_header_element(response, 7)
        notice["type"] = notice["type"].lower()

        _, response_required = self.get_header_element(response, 11)
        if not self.hasPhrase(["No response required"], response_required):
            notice["response"] = response_required
            _, response_date = self.get_header_element(response, 12)
            _, response_time = self.get_header_element(response, 13)

            if response_date and response_time:
                notice["response_dt"] = self.update_to_utc(
                    f"{response_date} {response_time}"
                )

        else:
            notice["response"] = "N"
            notice["response_dt"] = ""

        _, notice["service_provider"] = self.get_header_element(response, 15)
        body = ""
        for element in response.xpath(
            "//hr/following-sibling::*[self::p or self::div or normalize-space(text())]"
        ):
            body += element.extract()
        notice["body"] = body

        yield notice
