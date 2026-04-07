from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import scrapy
from gas_ebb.items import NoticeItem
from scrapy_splash import SplashRequest

FORMAT_DATE_TIME_STRING = "%m/%d/%Y %I:%M:%S %p"


def _clean_text_list(xs: list[str]) -> list[str]:
    out = []
    for x in xs:
        if x is None:
            continue
        s = x.strip()
        if s:
            out.append(s)
    return out


def _safe_get(xs: list[str], idx: int, default: str = "") -> str:
    return xs[idx] if 0 <= idx < len(xs) else default


def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, FORMAT_DATE_TIME_STRING)
    except ValueError:
        return None


class TexasEasternSpider(scrapy.Spider):
    name = "tetco_notices"
    allowed_domains = ["infopost.enbridge.com", "localhost"]
    start_urls = ["https://infopost.enbridge.com/infopost/TEHome.asp?Pipe=TE"]
    splash_args = {"wait": 1.5, "timeout": 90}

    def __init__(self, days_ago: int | str = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.days_ago = int(days_ago)
        except (TypeError, ValueError):
            self.days_ago = 1
        if self.days_ago < 0:
            self.days_ago = 0

    async def start(self):
        for url in self.start_urls:
            yield SplashRequest(
                url=url,
                callback=self.next_requests,
                endpoint="render.html",
                args=self.splash_args,
                dont_filter=True,
            )

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url=url,
                callback=self.next_requests,
                endpoint="render.html",
                args=self.splash_args,
                dont_filter=True,
            )

    def next_requests(self, response):
        for url in [
            "https://infopost.enbridge.com/infopost/NoticesList.asp?pipe=TE&type=CRI",
            "https://infopost.enbridge.com/infopost/NoticesList.asp?pipe=TE&type=NON",
        ]:
            yield SplashRequest(
                url=url,
                callback=self.parse_list,
                endpoint="render.html",
                args=self.splash_args,
                dont_filter=True,
            )

    def parse_list(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)

        rows = response.xpath(
            "//tr[.//a[contains(@href, 'NoticeDetail') or contains(@href, 'NoticesDetail') or contains(@href, 'Notice')]]"
        )

        for row in rows:
            posted_raw = row.xpath("normalize-space(.//td[2])").get()
            posted_dt = _parse_dt(posted_raw)
            if not posted_dt:
                continue

            if posted_dt.date() < cutoff_date:
                break

            href = row.xpath(".//td[last()-1]//a/@href").get()
            if not href:
                href = row.xpath(".//a/@href").get()
            if not href:
                continue

            detail_url = response.urljoin(href)
            yield SplashRequest(
                url=detail_url,
                callback=self.parse_detail,
                endpoint="render.html",
                args=self.splash_args,
                dont_filter=True,
                meta={"posted_dt": posted_dt},
            )

    def parse_detail(self, response):
        notice = NoticeItem()
        notice["kind"] = "pipeline"
        notice["url"] = response.url

        heading = _clean_text_list(
            response.xpath('//div[contains(@id, "headingData")]//text()').getall()
        )

        notice["tsp"] = _safe_get(heading, 0)
        notice["name"] = _safe_get(heading, 1)
        notice["critical"] = _safe_get(heading, 2)
        notice["effective_dt"] = _parse_dt(
            f"{_safe_get(heading, 3)} {_safe_get(heading, 4)}"
        )
        notice["end_dt"] = _parse_dt(
            f"{_safe_get(heading, 5)} {_safe_get(heading, 6)}"
        )
        notice["notice_id"] = _safe_get(heading, 7)
        notice["status"] = _safe_get(heading, 8)
        notice["type"] = _safe_get(heading, 9)

        posted_dt = _parse_dt(f"{_safe_get(heading, 10)} {_safe_get(heading, 11)}")
        notice["posted_dt"] = posted_dt or response.meta.get("posted_dt")

        notice["prior_id"] = _safe_get(heading, 12).strip()

        response_text = _safe_get(heading, 13)
        notice["response"] = response_text
        response_dt = _parse_dt(f"{_safe_get(heading, 14)} {_safe_get(heading, 15)}")
        if response_dt:
            notice["response_dt"] = response_dt

        notice["subject"] = _safe_get(heading, 16)

        bulletin_html = response.xpath('//div[contains(@id, "bulletin")]').getall()
        notice["body"] = "".join(bulletin_html).strip()

        yield notice
