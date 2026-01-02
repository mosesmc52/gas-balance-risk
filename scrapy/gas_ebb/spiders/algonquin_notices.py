from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import scrapy
from gas_ebb.items import Notice  # FIX: use your project item
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

    # Splash defaults (tune as needed)
    splash_args = {"wait": 1.5, "timeout": 90}

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

        # Only keep last 1 day (adjust as needed)
        cutoff_date = datetime.now().date() - timedelta(days=1)

        # More robust row selection (works even if tbody is missing)
        # We filter rows that contain a link to a detail page.
        rows = response.xpath(
            "//tr[.//a[contains(@href, 'NoticeDetail') or contains(@href, 'NoticesDetail') or contains(@href, 'Notice')]]"
        )

        self.logger.info("list url=%s rows=%s", response.url, len(rows))

        for row in rows:
            # Posted datetime is typically in the 2nd cell; normalize whitespace
            posted_raw = row.xpath("normalize-space(.//td[2])").get()
            posted_dt = _parse_dt(posted_raw)

            # If we cannot parse, skip row (don’t crash / don’t silently stop)
            if not posted_dt:
                continue

            if posted_dt.date() < cutoff_date:
                # assumes list is newest-first; safe early stop
                break

            href = row.xpath(".//td[last()-1]//a/@href").get()
            if not href:
                # fallback: try any link in the row
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
        notice = Notice()
        notice["kind"] = "pipeline"
        notice["url"] = response.url

        # headingData is typically a block of text nodes; strip and drop blanks
        heading = _clean_text_list(
            response.xpath('//div[contains(@id, "headingData")]//text()').getall()
        )

        # Defensive logging: if the page format changes, you will see it immediately.
        if len(heading) < 8:
            self.logger.warning(
                "Unexpected headingData length=%s url=%s", len(heading), response.url
            )

        # Preserve your original positional mapping, but with safe getters
        notice["tsp"] = _safe_get(heading, 0)
        notice["name"] = _safe_get(heading, 1)
        notice["notice_id"] = _safe_get(heading, 7)

        critical_label = _safe_get(heading, 2).lower()
        notice["critical"] = "Y" if "critical" in critical_label else "N"

        # These are usually date + time pairs in adjacent fields
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

        # Keep bulletin as HTML (your original approach), but pull the container(s) reliably
        bulletin_html = response.xpath('//div[contains(@id, "bulletin")]').getall()
        notice["body"] = "".join(bulletin_html).strip()

        yield notice
