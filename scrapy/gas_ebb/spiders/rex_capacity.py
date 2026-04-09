import json
import re
from datetime import datetime, timedelta, timezone

import scrapy
from gas_ebb.items import CapacityItem
from scrapy_splash import SplashRequest

REX_POINT_URL = "https://pipeline.tallgrassenergylp.com/Pages/Point.aspx?pipeline=501&type=OA"
REX_TSP = "876946001"
REX_NAME = "Rockies Express Pipeline"
DEFAULT_CYCLES = [
    "Best Available",
    "Timely",
    "Evening",
    "Intra-Day 1",
    "Intra-Day 2",
    "Intra-Day 3",
]

HEADER_MAP = {
    "Loc": "Loc",
    "Loc Name": "Loc_Name",
    "Flow Ind Desc": "Flow_Ind_Desc",
    "Loc Purp Desc": "Loc_Purp_Desc",
    "Loc/QTI Desc": "Loc_QTI_Desc",
    "Loc QTI Desc": "Loc_QTI_Desc",
    "Meas Basis Desc": "Meas_Basis_Desc",
    "Sched Qty": "Total_Scheduled_Quantity",
    "Scheduled Qty": "Total_Scheduled_Quantity",
    "Avail Qty": "Operationally_Available_Capacity",
    "Available Qty": "Operationally_Available_Capacity",
}


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


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


class RexCapacitySpider(scrapy.Spider):
    name = "rex_capacity"
    allowed_domains = ["pipeline.tallgrassenergylp.com", "localhost"]
    mongo_collection = "ebb_rex_capacity"
    mongo_unique_fields = [
        "Loc",
        "Eff_Gas_Day",
        "Cycle_Desc",
        "Flow_Ind_Desc",
        "Loc_QTI_Desc",
        "TSP",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raw_days_ago = kwargs.get("days_ago", "1")
        try:
            self.days_ago = max(1, int(raw_days_ago))
        except ValueError:
            self.days_ago = 1

        raw_cycles = kwargs.get("cycles")
        self.cycles = self._parse_cycles(raw_cycles)
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
        today_utc = datetime.now(timezone.utc).date()
        for offset in range(self.days_ago):
            target_date = today_utc - timedelta(days=offset)
            for cycle_desc in self.cycles:
                yield SplashRequest(
                    REX_POINT_URL,
                    callback=self.parse_page,
                    endpoint="execute",
                    args=self._splash_args(
                        lua_source=self.lua_script_query_capacity(),
                        target_date=target_date.strftime("%m/%d/%Y"),
                        cycle_desc=cycle_desc,
                    ),
                    headers=self.session_headers,
                    meta={
                        "target_date": target_date,
                        "cycle_desc": cycle_desc,
                    },
                    dont_filter=True,
                )

    def _splash_args(self, **extra):
        args = {"timeout": 90}
        if self.session_headers:
            args["headers"] = self.session_headers
        if self.session_cookies:
            args["session_cookies"] = self.session_cookies
        args.update(extra)
        return args

    def _parse_cycles(self, raw_cycles):
        if not raw_cycles:
            return DEFAULT_CYCLES

        requested = []
        for part in str(raw_cycles).split(","):
            value = _clean_text(part)
            if not value:
                continue
            for cycle in DEFAULT_CYCLES:
                if value.lower() == cycle.lower() and cycle not in requested:
                    requested.append(cycle)
        return requested or DEFAULT_CYCLES

    def lua_script_query_capacity(self):
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
                    var targetDate = %q;
                    var cycleDesc = %q;

                    var textInputs = Array.from(document.querySelectorAll("input[type='text']"));
                    var dateInput = textInputs.find(function(el) {
                        var key = (el.id || "") + " " + (el.name || "");
                        return /date/i.test(key);
                    }) || textInputs[0];
                    if (dateInput) {
                        dateInput.value = targetDate;
                        dateInput.dispatchEvent(new Event("change", {bubbles: true}));
                    }

                    var selects = Array.from(document.querySelectorAll("select"));
                    var cycleSelect = selects.find(function(el) {
                        var key = (el.id || "") + " " + (el.name || "");
                        return /cycle/i.test(key);
                    }) || selects[0];
                    if (cycleSelect) {
                        Array.from(cycleSelect.options).forEach(function(opt, idx) {
                            if (opt.text.replace(/\\s+/g, " ").trim().toLowerCase() === cycleDesc.toLowerCase()) {
                                cycleSelect.selectedIndex = idx;
                            }
                        });
                        cycleSelect.dispatchEvent(new Event("change", {bubbles: true}));
                    }

                    Array.from(document.querySelectorAll("input[type='radio'], input[type='checkbox']")).forEach(function(el) {
                        var label = "";
                        if (el.id) {
                            var labelEl = document.querySelector("label[for='" + el.id + "']");
                            if (labelEl) {
                                label = labelEl.textContent || "";
                            }
                        }
                        var key = [label, el.value || "", el.id || "", el.name || ""]
                            .join(" ")
                            .replace(/\\s+/g, " ")
                            .trim()
                            .toLowerCase();
                        if (key === cycleDesc.toLowerCase() || key.indexOf(cycleDesc.toLowerCase()) >= 0) {
                            el.checked = true;
                            el.dispatchEvent(new Event("click", {bubbles: true}));
                            el.dispatchEvent(new Event("change", {bubbles: true}));
                        }
                    });
                ]], args.target_date, args.cycle_desc))

                assert(splash:wait(0.5))

                local submit = splash:select("input[type='image'], input[type='submit'], button, a[id*='btn'], a[href*='javascript:__doPostBack']")
                if submit then
                    submit:mouse_click()
                    assert(splash:wait(2.0))
                else
                    splash:runjs([[
                        if (document.forms.length) {
                            document.forms[0].submit();
                        }
                    ]])
                    assert(splash:wait(2.0))
                end

                return {html = splash:html()}
            end
        """

    def _extract_table(self, response):
        for table in response.xpath("//table"):
            rows = table.xpath(".//tr")
            if not rows:
                continue

            header_index = None
            header_cells = []
            for idx, row in enumerate(rows):
                candidate_headers = [
                    _clean_text(text)
                    for text in row.xpath("./th//text() | ./td//text()").getall()
                    if _clean_text(text)
                ]
                if "Loc" in candidate_headers and "Loc Name" in candidate_headers:
                    header_index = idx
                    header_cells = candidate_headers
                    break

            if header_index is None:
                continue

            data_rows = []
            for row in rows[header_index + 1 :]:
                cells = [
                    _clean_text(text)
                    for text in row.xpath("./td//text()").getall()
                    if _clean_text(text)
                ]
                if len(cells) >= len(header_cells):
                    data_rows.append(cells[: len(header_cells)])

            if data_rows:
                return header_cells, data_rows

        return [], []

    def parse_page(self, response):
        headers, rows = self._extract_table(response)
        if not headers or not rows:
            self.logger.warning(
                "No REX capacity table found for target_date=%s cycle=%s",
                response.meta["target_date"],
                response.meta["cycle_desc"],
            )
            return

        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        target_date = response.meta["target_date"]
        cycle_desc = response.meta["cycle_desc"]

        page_text = _clean_text(" ".join(response.xpath("//body//text()").getall()))
        eff_date_match = re.search(r"Avail Cap Eff Date:\s*([0-9/*]+)", page_text)
        eff_time_match = re.search(r"Avail Cap Eff Time:\s*([0-9: ]+[AP]M|[0-9: ]+[A-Z]{3})", page_text)
        meas_match = re.search(r"Meas Basis Desc:\s*([^*]+?)(?:Loc Segment:|Loc:|\Z)", page_text)

        eff_date = (
            eff_date_match.group(1)
            if eff_date_match and "*" not in eff_date_match.group(1)
            else target_date.strftime("%m/%d/%Y")
        )
        eff_time = _clean_text(eff_time_match.group(1)) if eff_time_match else ""
        meas_basis = _clean_text(meas_match.group(1)) if meas_match else ""

        for row in rows:
            values = dict(zip(headers, row))
            item = CapacityItem()
            item["source_url"] = response.url
            item["downloaded_at_utc"] = downloaded_at_utc
            item["requested_post_date_utc"] = str(target_date)
            item["TSP"] = REX_TSP
            item["TSP_Name"] = REX_NAME
            item["Cycle_Desc"] = cycle_desc
            item["Eff_Gas_Day"] = eff_date
            if eff_time:
                item["Eff_Time"] = eff_time
            if meas_basis:
                item["Meas_Basis_Desc"] = meas_basis

            for raw_key, raw_value in values.items():
                item_key = HEADER_MAP.get(raw_key)
                if item_key and raw_value != "":
                    item[item_key] = raw_value

            if item.get("Loc"):
                yield item
