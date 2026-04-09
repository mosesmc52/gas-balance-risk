import json
from datetime import datetime, timedelta

import scrapy
from gas_ebb.items import NoticeItem
from scrapy_splash import SplashRequest

FORMAT_DATE_TIME_STRING = "%m/%d/%Y %I:%M:%S %p"
MAX_DETAIL_RETRY = 3


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def _parse_dt(value):
    value = _clean_text(value)
    if not value:
        return None
    try:
        return datetime.strptime(value, FORMAT_DATE_TIME_STRING)
    except ValueError:
        return None


def _parse_cookie_string(value):
    cookies = []
    for part in (value or "").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, cookie_value = part.split("=", 1)
        cookies.append({"name": name.strip(), "value": cookie_value.strip()})
    return cookies


def _normalize_splash_cookies(raw_json, raw_cookie_header):
    cookies = []

    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            parsed = None

        if isinstance(parsed, dict):
            for name, value in parsed.items():
                cookies.append({"name": str(name), "value": str(value)})
        elif isinstance(parsed, list):
            for cookie in parsed:
                if isinstance(cookie, dict) and cookie.get("name"):
                    cookies.append(
                        {
                            "name": str(cookie["name"]),
                            "value": str(cookie.get("value", "")),
                            **(
                                {"domain": str(cookie["domain"])}
                                if cookie.get("domain")
                                else {}
                            ),
                            **(
                                {"path": str(cookie["path"])}
                                if cookie.get("path")
                                else {}
                            ),
                        }
                    )

    if raw_cookie_header:
        cookies.extend(_parse_cookie_string(raw_cookie_header))

    deduped = []
    seen = set()
    for cookie in cookies:
        key = (cookie.get("name"), cookie.get("domain"), cookie.get("path"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cookie)
    return deduped


def _normalize_headers(raw_json, user_agent):
    headers = {}
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            headers.update({str(k): str(v) for k, v in parsed.items()})
    if user_agent and "User-Agent" not in headers:
        headers["User-Agent"] = str(user_agent)
    return headers


class RexNoticesSpider(scrapy.Spider):
    name = "rex_notices"
    allowed_domains = ["pipeline.tallgrassenergylp.com", "localhost"]
    mongo_collection = "ebb_rex_notices"
    mongo_unique_fields = ["notice_id"]
    start_urls = [
        "https://pipeline.tallgrassenergylp.com/Pages/Notices.aspx?pipeline=501&type=CRIT",
        "https://pipeline.tallgrassenergylp.com/Pages/Notices.aspx?pipeline=501&type=NONCRIT",
    ]

    def __init__(self, days_ago=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.days_ago = int(kwargs.get("days_ago", days_ago))
        except (TypeError, ValueError):
            self.days_ago = 1
        if self.days_ago < 0:
            self.days_ago = 0

        self.session_cookies = _normalize_splash_cookies(
            kwargs.get("cookies_json"),
            kwargs.get("cookie_header"),
        )
        self.session_headers = _normalize_headers(
            kwargs.get("headers_json"),
            kwargs.get("user_agent"),
        )

    async def start(self):
        for request in self._build_initial_requests():
            yield request

    def start_requests(self):
        yield from self._build_initial_requests()

    def _build_initial_requests(self):
        for url in self.start_urls:
            critical = "Y" if "type=CRIT" in url else "N"
            yield SplashRequest(
                url,
                callback=self.parse,
                endpoint="render.html",
                args=self._splash_args(wait=2.0),
                headers=self.session_headers,
                meta={"critical": critical},
                dont_filter=True,
            )

    def _splash_args(self, wait=1.0, **extra):
        args = {"wait": wait, "timeout": 90}
        if self.session_headers:
            args["headers"] = self.session_headers
        if self.session_cookies:
            args["session_cookies"] = self.session_cookies
        args.update(extra)
        return args

    def lua_script_click_detail_view(self):
        return """
            function main(splash, args)
                splash.images_enabled = false
                if args.headers then
                    splash:set_custom_headers(args.headers)
                end
                if args.session_cookies then
                    splash:init_cookies(args.session_cookies)
                end
                assert(splash:go(args.url))
                assert(splash:wait(1.0))

                splash:runjs(string.format([[
                    var link = document.getElementById(%q);
                    if (link) {
                        link.click();
                    }
                ]], args.anchor_id))

                assert(splash:wait(2.0))
                return {html = splash:html()}
            end
        """

    def parse(self, response):
        cutoff_date = datetime.now().date() - timedelta(days=self.days_ago)

        for row in response.xpath("//tr[.//a[starts-with(@id,'mainContent_GridView1')]]"):
            columns = [
                _clean_text(text)
                for text in row.xpath("./td//text()").getall()
                if _clean_text(text)
            ]
            if len(columns) < 7:
                continue

            posted_dt = _parse_dt(columns[2])
            if not posted_dt:
                posted_dt = _parse_dt(columns[3])
            if not posted_dt:
                continue

            if posted_dt.date() < cutoff_date:
                break

            anchor_id = row.xpath(".//a[starts-with(@id,'mainContent_GridView1')]/@id").get()
            subject = _clean_text(
                row.xpath(".//a[starts-with(@id,'mainContent_GridView1')]/text()").get()
            )
            if not anchor_id:
                continue

            yield SplashRequest(
                response.url,
                callback=self.parse_detail,
                endpoint="execute",
                args=self._splash_args(
                    lua_source=self.lua_script_click_detail_view(),
                    anchor_id=anchor_id,
                ),
                headers=self.session_headers,
                meta={
                    "subject": subject,
                    "anchor_id": anchor_id,
                    "attempt": 1,
                    "critical": response.meta.get("critical"),
                    "posted_dt": posted_dt,
                },
                dont_filter=True,
            )

    def get_field_content(self, response, field_id):
        value = response.xpath(
            f"normalize-space(//span[contains(@id, '{field_id}')])"
        ).get()
        return _clean_text(value)

    def parse_detail(self, response):
        notice_id = self.get_field_content(response, "mainContent_lblNoticeID")
        if not notice_id:
            if response.meta["attempt"] < MAX_DETAIL_RETRY:
                yield SplashRequest(
                    response.url,
                    callback=self.parse_detail,
                    endpoint="execute",
                    args=self._splash_args(
                        lua_source=self.lua_script_click_detail_view(),
                        anchor_id=response.meta["anchor_id"],
                    ),
                    headers=self.session_headers,
                    meta={
                        **response.meta,
                        "attempt": response.meta["attempt"] + 1,
                    },
                    dont_filter=True,
                )
            return

        notice = NoticeItem()
        notice["kind"] = "pipeline"
        notice["url"] = response.url
        notice["name"] = "Rockies Express Pipeline"
        notice["service_provider"] = "Rockies Express Pipeline"
        notice["tsp"] = "876946001"
        notice["notice_id"] = notice_id
        notice["subject"] = response.meta.get("subject")

        notc_type = self.get_field_content(response, "mainContent_lblNotcType")
        if notc_type:
            if "not a critical notice" in notc_type.lower():
                notice["critical"] = "N"
            elif "critical" in notc_type.lower():
                notice["critical"] = "Y"

        if "critical" not in notice:
            notice["critical"] = response.meta.get("critical")

        notice["effective_dt"] = _parse_dt(
            self.get_field_content(response, "mainContent_lblNotcEffDt")
        )
        notice["end_dt"] = _parse_dt(
            self.get_field_content(response, "mainContent_lblNotcEndDt")
        )
        notice["posted_dt"] = response.meta.get("posted_dt")

        status = self.get_field_content(response, "mainContent_lblStatus")
        if status:
            notice["status"] = status.lower()

        subtype = self.get_field_content(response, "mainContent_lblNotcSub")
        if subtype:
            notice["type"] = subtype.lower()

        prior_id = self.get_field_content(response, "mainContent_lblPrior")
        if prior_id:
            notice["prior_id"] = prior_id

        response_required = self.get_field_content(response, "mainContent_lblRspReq")
        if response_required and "no response" not in response_required.lower():
            notice["response"] = "Y"
        else:
            notice["response"] = "N"

        pdf_path = response.xpath(
            "//div[contains(@id, 'mainContent_pnlPDF')]//iframe/@src"
        ).get()
        if pdf_path:
            notice["body"] = response.urljoin(pdf_path)
        else:
            body_html = response.xpath(
                "//table[contains(@id, 'mainContent_DetailsView1')]"
            ).get()
            notice["body"] = body_html or _clean_text(response.text)

        yield notice
