import csv
import io
from datetime import datetime, timezone

import scrapy
from gas_ebb.items import CapacityItem  # FIX: use your project item


class AlgonquinCapacitySpider(scrapy.Spider):
    name = "algonquin_capacity"
    allowed_domains = ["rtba.enbridge.com"]
    start_url = (
        "https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu=AG&Type=OA"
    )
    mongo_collection = "ebb_algonquin_capacity"
    mongo_unique_fields = ["Loc_Name", "Post_Date", "TSP"]

    def start_requests(self):
        yield scrapy.Request(self.start_url, callback=self.parse_page)

    def parse_page(self, response):
        # 1) Extract required ASP.NET hidden fields
        def hv(name: str) -> str:
            return response.css(f'input[name="{name}"]::attr(value)').get() or ""

        viewstate = hv("__VIEWSTATE")
        viewstate_gen = hv("__VIEWSTATEGENERATOR")
        event_validation = hv("__EVENTVALIDATION")

        # Some pages also use these; harmless if empty
        viewstate_encrypted = hv("__VIEWSTATEENCRYPTED")

        event_target = "ctl00$MainContent$ctl01$oaDefault$hlDown$LinkButton1"
        event_argument = ""

        # 2) Build form data for the postback
        formdata = {
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": event_argument,
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstate_gen,
            "__EVENTVALIDATION": event_validation,
        }
        if viewstate_encrypted:
            formdata["__VIEWSTATEENCRYPTED"] = viewstate_encrypted

        yield scrapy.FormRequest(
            url=response.url,
            formdata=formdata,
            method="POST",
            callback=self.parse_csv,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": response.url,
            },
            dont_filter=True,
        )

    def parse_csv(self, response):
        content_type = (
            response.headers.get(b"Content-Type", b"")
            .decode("utf-8", errors="ignore")
            .lower()
        )
        disp = response.headers.get(b"Content-Disposition", b"").decode(
            "utf-8", errors="ignore"
        )

        if "text/csv" not in content_type and "attachment" not in disp.lower():
            self.logger.warning(
                "Did not receive CSV. Content-Type=%s Disposition=%s",
                content_type,
                disp,
            )
            self.logger.warning("First 200 bytes: %r", response.body[:200])
            yield {
                "error": "not_csv",
                "content_type": content_type,
                "disposition": disp,
                "url": response.url,
            }
            return

        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Decode CSV bytes. If you ever see odd characters, switch to "utf-8-sig".
        text = response.body.decode("utf-8", errors="replace")

        # Use DictReader to map header -> value
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            # Optional: normalize whitespace
            row = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

            item = CapacityItem()
            item["source_url"] = response.url
            item["downloaded_at_utc"] = downloaded_at_utc

            # Assign all CSV columns that exist in the row
            # (This will work even if Enbridge adds/removes columns later)
            for k, v in row.items():
                item[k] = v

            yield item
