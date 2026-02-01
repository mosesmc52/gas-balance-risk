import re
from urllib.parse import urlencode

import crawlers.items
import scrapy


class NNGPSpider(scrapy.Spider):
    # http://www.northernnaturalgas.com/infopostings/pages/atAGlance.aspx
    name = "nngp"
    allowed_domains = ["northernnaturalgas.com"]

    def hasPhrase(phrases=[], text=""):
        return any([phrase for phrase in phrases if phrase.lower() in text.lower()])

    def start_requests(self):
        url = "https://www.northernnaturalgas.com/infopostings/pages/atAGlance.aspx"
        yield scrapy.Request(
            url,
            callback=self.parse,
        )

    def parse(self, response):

        rows = response.xpath(
            "//table[contains(@id, 'ctl00_ctl45_g_0a028a31_2fe6_4c01_850a_eb67567d5342_gvNonCritical_ctl00')]/tbody/tr"
        )
        for row in rows:
            link_selector = row.xpath("td/a/@onclick")
            if len(link_selector):
                id = re.search(r"ID=[\d]*", link_selector.get(), re.IGNORECASE)[
                    0
                ].split("=")[1]

                url = f"https://www.northernnaturalgas.com/_layouts/15/NoticePopup.aspx?ID={id}"
                yield scrapy.Request(
                    url,
                    callback=self.detail,
                )

        rows = response.xpath(
            "//table[contains(@id, 'ctl00_ctl45_g_f42a2d08_fbb7_4220_a12d_5cecb2cc46a9_gvNonCritical_ctl00')]/tbody/tr"
        )
        for row in rows:
            link_selector = row.xpath("td/a/@onclick")
            if len(link_selector):
                id = re.search(r"ID=[\d]*", link_selector.get(), re.IGNORECASE)[
                    0
                ].split("=")[1]

                url = f"https://www.northernnaturalgas.com/_layouts/15/NoticePopup.aspx?ID={id}"
                yield scrapy.Request(
                    url,
                    callback=self.detail,
                )

    def detail(self, response):
        notice = crawlers.items.Notice()
        notice["kind"] = "pipeline"
        notice["url"] = response.url

        header_table = response.xpath(
            "//table[contains(@class, 'noticeDetailMain')]/tr"
        )[0]
        left_header = header_table.xpath("td")[0]
        for row in left_header.xpath("table/tr"):
            data = row.xpath("td/span/text()").extract()
            if data[0].strip().lower() == "tsp name:":
                notice["name"] = data[1]
            # elif data[0].strip().lower() == "tsp:":
            #     notice["notice_id"] = data[1]
            elif data[0].strip().lower() == "notice type:":
                notice["type"] = data[1]
            elif data[0].strip().lower() == "subject":
                notice["subject"] = data[1]
            elif data[0].strip().lower() == "critical":
                notice["critical"] = data[1]
            elif data[0].strip().lower() == "reason:":
                notice["reason"] = data[1]
            elif data[0].strip().lower() == "location:":
                notice["location"] = data[1]
            elif "notice id" in data[0].strip().lower():
                notice["notice_id"] = re.sub(
                    r"[^\x00-\x7f]", r"", data[0].split(":")[1]
                ).strip()

            print(row.xpath("td/span/text()").extract())

        right_header = header_table.xpath("td")[1]
        for row in right_header.xpath("table/tr"):
            if data[0].strip().lower() == "post date/time:":
                notice["posted_dt"] = data[1]
            elif data[0].strip().lower() == "notice effective date/time:":
                notice["effective_dt"] = data[1]
            elif data[0].strip().lower() == "notice end date/time:":
                notice["end_dt"] = data[1]
            elif data[0].strip().lower() == "notice status:":
                notice["status"] = data[1]

        body_table = response.xpath(
            "//table[contains(@class, 'noticeDetailMain')]/tr/td"
        )[2].xpath("table")
        notice["body"] = body_table.extract()
