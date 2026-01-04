import csv
import io
from datetime import datetime, timedelta, timezone

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Scrapy passes -a days_ago=7 as a string
        raw = kwargs.get("days_ago", "1")
        try:
            self.days_ago = max(1, int(raw))
        except ValueError:
            self.days_ago = 1

    def start_requests(self):
        """
        Fetch the page once per day so we can extract fresh VIEWSTATE per request,
        then submit the download postback with the date set for that day.
        """
        today_utc = datetime.now(timezone.utc).date()
        for offset in range(self.days_ago):
            d = today_utc - timedelta(days=offset)
            yield scrapy.Request(
                self.start_url,
                callback=self.parse_page,
                meta={"target_date": d},
                dont_filter=True,
            )

    def parse_page(self, response):
        target_date = response.meta.get("target_date")  # datetime.date

        # 1) Extract required ASP.NET hidden fields
        def hv(name: str) -> str:
            return response.css(f'input[name="{name}"]::attr(value)').get() or ""

        viewstate = hv("__VIEWSTATE")
        viewstate_gen = hv("__VIEWSTATEGENERATOR")
        event_validation = hv("__EVENTVALIDATION")
        viewstate_encrypted = hv("__VIEWSTATEENCRYPTED")  # optional

        event_target = "ctl00$MainContent$ctl01$oaDefault$hlDown$LinkButton1"
        event_argument = ""

        # 2) Build form data for the postback
        formdata = {
            "ctl00_ScriptManager111_HiddenField": "",
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": event_argument,
            "__LASTFOCUS": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstate_gen,
            "__EVENTVALIDATION": event_validation,
        }
        if viewstate_encrypted:
            formdata["__VIEWSTATEENCRYPTED"] = viewstate_encrypted

        # 3) Set the page's date picker input so the CSV corresponds to target_date
        #
        # Many Enbridge/Telerik pages use RadDatePicker whose editable input is:
        #   name="...$<something>$dateInput"
        # and value looks like: "YYYY-MM-DD-00-00-00" (matches what you pasted).
        #
        # We *discover* the right field name from the DOM to avoid hardcoding.
        date_value = datetime(
            target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc
        ).strftime("%Y-%m-%d-00-00-00")

        # Find likely Telerik date inputs
        date_input_names = response.css(
            'input[name$="$dateInput"]::attr(name)'
        ).getall()

        # Prefer ones that look like they belong to oaDefault / posting date widgets
        preferred = None
        for nm in date_input_names:
            nml = (nm or "").lower()
            if "oadefault" in nml or "post" in nml or "date" in nml:
                preferred = nm
                break

        if preferred:
            formdata[preferred] = date_value
        elif date_input_names:
            # fallback: just take the first dateInput found
            formdata[date_input_names[0]] = date_value
        else:
            # If we cannot find a date input, we still attempt the download.
            # This will likely download the site's default date (often "today").
            self.logger.warning(
                "No Telerik $dateInput field found; downloading default date for target_date=%s",
                target_date,
            )

        yield scrapy.FormRequest(
            url=response.url,
            formdata=formdata,
            method="POST",
            callback=self.parse_csv,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": response.url,
            },
            meta={"target_date": target_date},
            dont_filter=True,
        )

    def parse_csv(self, response):
        target_date = response.meta.get("target_date")

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
                "target_date": str(target_date) if target_date else None,
            }
            return

        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Decode CSV bytes. If you ever see odd characters, switch to "utf-8-sig".
        text = response.body.decode("utf-8", errors="replace")

        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            row = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

            item = CapacityItem()
            item["source_url"] = response.url
            item["downloaded_at_utc"] = downloaded_at_utc

            # Helpful metadata for debugging / partitioning in Mongo
            if target_date:
                item["requested_post_date_utc"] = str(target_date)

            for k, v in row.items():
                item[k] = v

            yield item
