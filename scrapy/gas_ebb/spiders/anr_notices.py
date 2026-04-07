from datetime import datetime, timedelta

import scrapy
from gas_ebb.items import NoticeItem
from scrapy_splash import SplashRequest

DATE_FORMATS = (
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
)

SPLASH_LUA_CLICK_TAB = """
function main(splash, args)
  splash.private_mode_enabled = false
  splash.resource_timeout = 90.0
  splash:set_user_agent(args.user_agent)
  assert(splash:go(args.url))
  assert(splash:wait(args.initial_wait or 5.0))

  local js = string.format([[
    (function() {
      var link = document.querySelector(%q);
      if (link) {
        link.click();
        return true;
      }
      return false;
    })();
  ]], args.tab_selector)

  local ok, clicked = splash:evaljs(js)
  if ok == nil then
    error(clicked)
  end

  assert(splash:wait(args.post_click_wait or 5.0))
  return {
    html = splash:html(),
    clicked = clicked
  }
end
"""


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def _parse_dt(value):
    value = _clean_text(value)
    if not value:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


class ANRNoticesSpider(scrapy.Spider):
    name = "anr_notices"
    allowed_domains = ["ebb.tceconnects.com", "localhost"]
    custom_settings = {
        "DOWNLOAD_TIMEOUT": 180,
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "scrapy_user_agents.middlewares.RandomUserAgentMiddleware": None,
            "scrapy_splash.SplashCookiesMiddleware": 723,
            "scrapy_splash.SplashMiddleware": 725,
            "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 810,
        },
    }
    mongo_collection = "ebb_anr_notices"
    mongo_unique_fields = ["notice_id"]
    overview_url = (
        "https://ebb.tceconnects.com/infopost/OperationalOverview.aspx?v=1.1&assetid=3005"
    )
    fixed_user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    tabs = {
        "Y": 'a[href="#tabCriticalNotices"]',
        "N": 'a[href="#tabNonCriticalNotices"]',
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
        cookiejar_id = 1
        for critical, tab_selector in self.tabs.items():
            yield SplashRequest(
                url=self.overview_url,
                endpoint="execute",
                callback=self.parse_overview_tab,
                args={
                    "lua_source": SPLASH_LUA_CLICK_TAB,
                    "url": self.overview_url,
                    "tab_selector": tab_selector,
                    "initial_wait": 5.0,
                    "post_click_wait": 5.0,
                    "timeout": 90,
                    "user_agent": self.fixed_user_agent,
                },
                headers={"User-Agent": self.fixed_user_agent},
                meta={"critical": critical, "cookiejar": cookiejar_id},
                dont_filter=True,
            )
            cookiejar_id += 1

    def parse_overview_tab(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)
        critical = response.meta["critical"]

        rows = response.xpath(
            '//table[@id="CriticalNotices" or @id="NonCriticalNotices"]//tr[contains(@class, "jqgrow")]'
        )

        if not rows:
            self.logger.warning(
                "No ANR notice rows found after clicking tab critical=%s clicked=%r",
                critical,
                response.data.get("clicked"),
            )

        for row in rows:
            notice_id = _clean_text(row.xpath("./@id").get())
            cells = [
                _clean_text(x)
                for x in row.xpath("./td//text()[normalize-space()]").getall()
            ]
            if len(cells) < 2 or not notice_id:
                continue

            notice_title = cells[1] if len(cells) > 1 else ""
            posted_raw = cells[2] if len(cells) > 2 else ""
            posted_dt = _parse_dt(posted_raw)

            if posted_dt and posted_dt.date() < cutoff_date:
                continue

            detail_url = (
                "https://ebb.tceconnects.com/infopost/ReportViewer.aspx"
                f"?/InfoPost/NoticesSubreport&pNoticeId={notice_id}&AssetNbr=3005"
            )

            yield SplashRequest(
                url=detail_url,
                endpoint="render.html",
                callback=self.parse_detail,
                args={"wait": 2.0, "timeout": 90},
                headers={
                    "Referer": self.overview_url,
                    "User-Agent": self.fixed_user_agent,
                },
                meta={
                    "critical": critical,
                    "notice_id": notice_id,
                    "notice_title": notice_title,
                    "posted_dt": posted_dt,
                    "cookiejar": response.meta["cookiejar"],
                },
                dont_filter=True,
            )

    def _extract_label_map(self, response):
        data = {}
        for row in response.xpath("//tr[td]"):
            cells = [
                _clean_text(x)
                for x in row.xpath("./td//text()[normalize-space()]").getall()
            ]
            if len(cells) < 2:
                continue
            label = cells[0].rstrip(":").strip().lower()
            value = " ".join(cells[1:]).strip()
            if label and value and label not in data:
                data[label] = value
        return data

    def parse_detail(self, response):
        notice = NoticeItem()
        notice["kind"] = "pipeline"
        notice["name"] = "ANR Pipeline Company"
        notice["tsp"] = "ANR"
        notice["service_provider"] = "ANR Pipeline Company"
        notice["url"] = response.url
        notice["critical"] = response.meta["critical"]
        notice["notice_id"] = response.meta["notice_id"]
        notice["posted_dt"] = response.meta.get("posted_dt")

        labels = self._extract_label_map(response)

        notice["subject"] = (
            labels.get("subject")
            or response.meta.get("notice_title")
            or _clean_text(
                response.xpath("//title/text() | //h1/text() | //h2/text()").get()
            )
        )
        notice["type"] = labels.get("notice type") or labels.get("type")
        notice["status"] = labels.get("status")
        notice["prior_id"] = labels.get("prior notice id") or labels.get("prior id")
        notice["response"] = labels.get("response required") or labels.get("response")
        notice["reason"] = labels.get("reason")
        notice["location"] = labels.get("location")
        notice["contact_phone"] = labels.get("contact phone")
        notice["published_by_email"] = labels.get("published by email")

        effective_dt = labels.get("notice effective date/time") or labels.get(
            "effective date/time"
        )
        end_dt = labels.get("notice end date/time") or labels.get("end date/time")
        response_dt = labels.get("response date") or labels.get("response date/time")
        posted_dt = labels.get("post date/time") or labels.get("posted date/time")

        parsed_effective = _parse_dt(effective_dt)
        if parsed_effective:
            notice["effective_dt"] = parsed_effective

        parsed_end = _parse_dt(end_dt)
        if parsed_end:
            notice["end_dt"] = parsed_end

        parsed_response_dt = _parse_dt(response_dt)
        if parsed_response_dt:
            notice["response_dt"] = parsed_response_dt
            if not notice.get("response"):
                notice["response"] = "Y"

        if not notice.get("posted_dt"):
            parsed_posted = _parse_dt(posted_dt)
            if parsed_posted:
                notice["posted_dt"] = parsed_posted

        body_html = response.xpath("//body").get() or response.text
        notice["body"] = body_html.strip()

        yield notice
