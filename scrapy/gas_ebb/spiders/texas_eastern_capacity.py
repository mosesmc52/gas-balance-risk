import csv
import io
from datetime import datetime, timedelta, timezone

import scrapy
from gas_ebb.items import CapacityItem


class TexasEasternCapacitySpider(scrapy.Spider):
    name = "tetco_capacity"
    allowed_domains = ["rtba.enbridge.com"]
    start_url = (
        "https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu=TE&Type=OA"
    )
    mongo_collection = "ebb_texas_eastern_capacity"
    mongo_unique_fields = ["Loc", "Post_Date", "Post_Time", "Cycle_Desc", "TSP"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        raw = kwargs.get("days_ago", "1")
        try:
            self.days_ago = max(1, int(raw))
        except ValueError:
            self.days_ago = 1

    async def start(self):
        for request in self._build_initial_requests():
            yield request

    def start_requests(self):
        yield from self._build_initial_requests()

    def _build_initial_requests(self):
        today_utc = datetime.now(timezone.utc).date()
        for offset in range(self.days_ago):
            target_date = today_utc - timedelta(days=offset)
            yield scrapy.Request(
                self.start_url,
                callback=self.parse_page,
                meta={"target_date": target_date},
                dont_filter=True,
            )

    def parse_page(self, response):
        target_date = response.meta.get("target_date")

        def hidden_value(name: str) -> str:
            return response.css(f'input[name="{name}"]::attr(value)').get() or ""

        formdata = {
            "ctl00_ScriptManager111_HiddenField": hidden_value(
                "ctl00_ScriptManager111_HiddenField"
            ),
            "__EVENTTARGET": "ctl00$MainContent$ctl01$oaDefault$hlDown$LinkButton1",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": hidden_value("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": hidden_value("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION": hidden_value("__EVENTVALIDATION"),
        }

        viewstate_encrypted = hidden_value("__VIEWSTATEENCRYPTED")
        if viewstate_encrypted:
            formdata["__VIEWSTATEENCRYPTED"] = viewstate_encrypted

        date_input_names = response.css(
            'input[name$="$dateInput"]::attr(name)'
        ).getall()
        target_date_value = datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            tzinfo=timezone.utc,
        ).strftime("%Y-%m-%d-00-00-00")
        display_date_value = f"{target_date.month}/{target_date.day}/{target_date.year}"

        preferred_date_input = None
        for name in date_input_names:
            normalized = (name or "").lower()
            if "oadefault" in normalized or "date" in normalized:
                preferred_date_input = name
                break

        if preferred_date_input:
            formdata[preferred_date_input] = target_date_value

            base_name = preferred_date_input[: -len("$dateInput")]
            formdata.setdefault(base_name, target_date.strftime("%Y-%m-%d"))
            formdata.setdefault(
                base_name.replace("$", "_") + "_ClientState",
                "",
            )
            formdata.setdefault(preferred_date_input.replace("$", "_") + "_ClientState", "")
        elif date_input_names:
            fallback_name = date_input_names[0]
            formdata[fallback_name] = target_date_value
            formdata.setdefault(
                fallback_name.replace("$", "_") + "_ClientState",
                "",
            )
        else:
            self.logger.warning(
                "No Telerik $dateInput field found; downloading default date for target_date=%s",
                target_date,
            )

        hidden_date_inputs = response.css(
            'input[name*="$rdpDate"][type="text"]::attr(name)'
        ).getall()
        for name in hidden_date_inputs:
            if name.endswith("$dateInput"):
                continue
            formdata[name] = target_date.strftime("%Y-%m-%d")

        hidden_date_change_name = "ctl00$MainContent$hidDateChange"
        if response.css(f'input[name="{hidden_date_change_name}"]').get():
            formdata[hidden_date_change_name] = display_date_value

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
        disposition = response.headers.get(b"Content-Disposition", b"").decode(
            "utf-8", errors="ignore"
        )

        if "text/csv" not in content_type and "attachment" not in disposition.lower():
            self.logger.warning(
                "Did not receive Texas Eastern CSV. Content-Type=%s Disposition=%s",
                content_type,
                disposition,
            )
            self.logger.warning("First 200 bytes: %r", response.body[:200])
            yield {
                "error": "not_csv",
                "content_type": content_type,
                "disposition": disposition,
                "url": response.url,
                "target_date": str(target_date) if target_date else None,
            }
            return

        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        text = response.body.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            row = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

            item = CapacityItem()
            item["source_url"] = response.url
            item["downloaded_at_utc"] = downloaded_at_utc

            if target_date:
                item["requested_post_date_utc"] = str(target_date)

            for key, value in row.items():
                item[key] = value

            yield item
