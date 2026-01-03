from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import scrapy
from gas_ebb.items import NoticeItem
from scrapy_splash import SplashRequest

FORMAT_DATE_TIME_STRING = "%m/%d/%Y %I:%M:%S %p"


def _clean_text_list(xs: list[str]) -> list[str]:
    """Strip whitespace and drop empty strings."""
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


class AlgonquinNoticesSpider(scrapy.Spider):
    name = "algonquin_notices"
    allowed_domains = ["infopost.enbridge.com", "localhost"]
    start_urls = ["https://infopost.enbridge.com/infopost/AGHome.asp?Pipe=AG"]
    mongo_collection = "ebb_algonquin_notices"
    mongo_unique_fields = ["tsp", "notice_id", "posted_dt"]

    # Splash defaults (tune as needed)
    splash_args = {"wait": 1.5, "timeout": 90}

    # ---- NEW: CLI-configurable cutoff ----
    # Run like:
    #   scrapy crawl algonquin_notices -a cutoff_days=1
    #   scrapy crawl algonquin_notices -a cutoff_days=3
    def __init__(self, cutoff_days: int | str = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.cutoff_days = int(cutoff_days)
        except (TypeError, ValueError):
            self.cutoff_days = 1
        if self.cutoff_days < 0:
            self.cutoff_days = 0

    # Scrapy 2.13+ preferred entrypoint (keeps you future-proof)
    async def start(self):
        for url in self.start_urls:
            yield SplashRequest(
                url=url,
                callback=self.next_requests,
                endpoint="render.html",
                args=self.splash_args,
                dont_filter=True,
            )

    # Backward compatibility (older Scrapy versions still call start_requests)
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
            "https://infopost.enbridge.com/infopost/NoticesList.asp?pipe=AG&type=CRI",
            "https://infopost.enbridge.com/infopost/NoticesList.asp?pipe=AG&type=NON",
        ]:
            yield SplashRequest(
                url=url,
                callback=self.parse_list,
                endpoint="render.html",
                args=self.splash_args,
                dont_filter=True,
            )

    def parse_list(self, response):
        # ---- UPDATED: use CLI-configured cutoff_days ----
        cutoff_date = datetime.now().date() - timedelta(days=self.cutoff_days)

        rows = response.xpath(
            "//tr[.//a[contains(@href, 'NoticeDetail') or contains(@href, 'NoticesDetail') or contains(@href, 'Notice')]]"
        )

        self.logger.info(
            "list url=%s rows=%s cutoff_days=%s cutoff_date=%s",
            response.url,
            len(rows),
            self.cutoff_days,
            cutoff_date,
        )

        for row in rows:
            posted_raw = row.xpath("normalize-space(.//td[2])").get()
            posted_dt = _parse_dt(posted_raw)

            if not posted_dt:
                continue

            if posted_dt.date() < cutoff_date:
                # assumes list is newest-first; safe early stop
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

        if len(heading) < 8:
            self.logger.warning(
                "Unexpected headingData length=%s url=%s", len(heading), response.url
            )

        notice["tsp"] = _safe_get(heading, 0)
        notice["name"] = _safe_get(heading, 1)
        notice["notice_id"] = _safe_get(heading, 7)

        critical_label = _safe_get(heading, 2).lower()
        notice["critical"] = "Y" if "critical" in critical_label else "N"

        notice["effective_dt"] = _parse_dt(
            f"{_safe_get(heading, 3)} {_safe_get(heading, 4)}"
        )
        notice["end_dt"] = _parse_dt(f"{_safe_get(heading, 5)} {_safe_get(heading, 6)}")

        notice["status"] = _safe_get(heading, 8).lower()
        notice["type"] = _safe_get(heading, 9).lower()

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
