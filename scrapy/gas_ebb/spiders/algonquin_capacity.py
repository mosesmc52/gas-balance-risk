from urllib.parse import urlencode

import scrapy


class AlgonquinCapacitySpider(scrapy.Spider):
    name = "algonquin_capacity"
    allowed_domains = ["rtba.enbridge.com"]
    start_url = (
        "https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu=AG&Type=OA"
    )

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

        # IMPORTANT: carry cookies/session automatically by using the same Scrapy cookiejar
        yield scrapy.FormRequest(
            url=response.url,  # post back to same URL
            formdata=formdata,
            method="POST",
            callback=self.save_csv,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": response.url,
            },
            dont_filter=True,
        )

    def save_csv(self, response):
        # 3) The response should be a CSV download
        content_type = (
            response.headers.get(b"Content-Type", b"")
            .decode("utf-8", errors="ignore")
            .lower()
        )
        disp = response.headers.get(b"Content-Disposition", b"").decode(
            "utf-8", errors="ignore"
        )

        # If you get HTML here, it usually means one of the hidden fields was missing/stale,
        # or the control requires additional form inputs. Save it to inspect.
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

        # Choose a filename (you can also parse Content-Disposition if present)
        filename = "algonquin_oa.csv"
        yield {
            "csv_filename": filename,
            "csv_bytes": len(response.body),
            "content_type": content_type,
        }

        # If you want to save to disk inside the spider:
        with open(filename, "wb") as f:
            f.write(response.body)
