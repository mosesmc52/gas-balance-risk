from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import scrapy
from gas_ebb.items import CapacityItem


def _clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


class NorthernNaturalCapacitySpider(scrapy.Spider):
    name = "nngp_capacity"
    allowed_domains = ["northernnaturalgas.com"]
    start_urls = [
        "https://www.northernnaturalgas.com/infopostings/Capacity/Pages/OperationallyAvailable.aspx",
        "https://www.northernnaturalgas.com/infopostings/Capacity/Pages/Unsubscribed.aspx",
    ]
    mongo_collection = "ebb_northern_natural_capacity"
    mongo_unique_fields = ["Loc", "Post_Date", "Cycle_Desc", "Cap_Type_Desc", "source_url"]

    OAC_POSITIONS = {
        0: "Loc",
        1: "Loc_Zn",
        2: "Loc_Name",
        3: "Flow_Ind_Desc",
        4: "Loc_QTI_Desc",
        5: "Loc_Purp_Desc",
        7: "Total_Design_Capacity",
        8: "Operating_Capacity",
        9: "Total_Scheduled_Quantity",
        10: "Operationally_Available_Capacity",
        11: "IT",
        12: "Qty_Reason",
    }

    UNSUB_FIELD_MAP = {
        "location": "Loc",
        "loc": "Loc",
        "location name": "Loc_Name",
        "loc name": "Loc_Name",
        "zone": "Loc_Zn",
        "loc zn": "Loc_Zn",
        "flow ind": "Flow_Ind_Desc",
        "flow direction": "Flow_Ind_Desc",
        "loc qti": "Loc_QTI_Desc",
        "loc/qti": "Loc_QTI_Desc",
        "location type": "Loc_Purp_Desc",
        "loc type": "Loc_Purp_Desc",
        "capacity type": "Cap_Type_Desc",
        "all qty avail": "All_Qty_Avail",
        "design capacity": "Total_Design_Capacity",
        "operating capacity": "Operating_Capacity",
        "scheduled quantity": "Total_Scheduled_Quantity",
        "unsubscribed capacity": "Operationally_Available_Capacity",
        "available quantity": "Operationally_Available_Capacity",
        "available capacity": "Operationally_Available_Capacity",
        "it": "IT",
        "qty reason": "Qty_Reason",
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
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        if "OperationallyAvailable.aspx" in response.url:
            yield from self.parse_oac_index(response)
            return

        yield from self.parse_unsubscribed_current(response)

    def parse_oac_index(self, response):
        cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=self.days_ago)

        for row in response.xpath(
            "//table[contains(@class, 'rgMasterTable')]//tr[contains(@class, 'rgRow') or contains(@class, 'rgAltRow')]"
        ):
            cells = [_clean_text(x) for x in row.xpath("./td[1]//text()").getall()]
            gas_date_text = next((x for x in cells if x), "")
            if not gas_date_text:
                continue

            try:
                gas_date = datetime.strptime(gas_date_text, "%m/%d/%Y").date()
            except ValueError:
                continue

            if gas_date < cutoff_date:
                continue

            for link in row.xpath("./td[position() > 1]/a"):
                href = link.xpath("./@href").get()
                cycle_desc = _clean_text(" ".join(link.xpath(".//text()").getall()))
                if not href or not cycle_desc:
                    continue

                yield scrapy.Request(
                    url=response.urljoin(href),
                    callback=self.parse_oac_report,
                    meta={
                        "gas_date": gas_date.isoformat(),
                        "cycle_desc": cycle_desc.replace(" Report", "").strip(),
                    },
                    dont_filter=True,
                )

    def parse_oac_report(self, response):
        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        parsed = urlparse(response.url)
        params = parse_qs(parsed.query)

        cycle_desc = response.meta.get("cycle_desc")
        gas_date = response.meta.get("gas_date")
        cycle_code = params.get("Cycle", [""])[0]

        for row in response.xpath(
            "//table[contains(@id, 'dg_OAC_NAESB30_ctl00')]//tr[contains(@class, 'rgRow') or contains(@class, 'rgAltRow')]"
        ):
            values = [
                _clean_text(" ".join(td.xpath(".//text()").getall()))
                for td in row.xpath("./td")
            ]
            if len(values) != 13:
                continue

            if not any(values):
                continue

            if values[0] == "" and values[2]:
                continue

            item = CapacityItem()
            item["source_url"] = response.url
            item["downloaded_at_utc"] = downloaded_at_utc
            item["requested_post_date_utc"] = gas_date
            item["Post_Date"] = gas_date
            item["Eff_Gas_Day"] = gas_date
            item["Cycle_Desc"] = cycle_desc or cycle_code
            item["Cap_Type_Desc"] = "Operationally Available"
            item["TSP"] = "784158214"
            item["TSP_Name"] = "Northern Natural Gas Company"

            for index, field_name in self.OAC_POSITIONS.items():
                value = values[index]
                if value:
                    item[field_name] = value

            if len(item) > 6:
                yield item

    def parse_unsubscribed_current(self, response):
        downloaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        requested_day = datetime.now(timezone.utc).date().isoformat()
        yielded = False

        for table in response.xpath("//table[.//tr]"):
            header_cells = table.xpath(".//tr[1]/th//text() | .//tr[1]/td//text()").getall()
            headers = [_clean_text(x).lower() for x in header_cells if _clean_text(x)]
            if len(headers) < 2:
                continue

            mapped_headers = [self.UNSUB_FIELD_MAP.get(h) for h in headers]
            if not any(mapped_headers):
                continue

            for row in table.xpath(".//tr[position() > 1]"):
                values = [
                    _clean_text(" ".join(td.xpath(".//text()").getall()))
                    for td in row.xpath("./td")
                ]
                if len(values) < 2:
                    continue

                item = CapacityItem()
                item["source_url"] = response.url
                item["downloaded_at_utc"] = downloaded_at_utc
                item["requested_post_date_utc"] = requested_day
                item["Post_Date"] = requested_day
                item["Cap_Type_Desc"] = "Unsubscribed"
                item["TSP"] = "784158214"
                item["TSP_Name"] = "Northern Natural Gas Company"

                for header, value in zip(headers, values):
                    field_name = self.UNSUB_FIELD_MAP.get(header)
                    if field_name and value:
                        item[field_name] = value

                if len(item) > 6:
                    yielded = True
                    yield item

        if not yielded:
            self.logger.warning(
                "Northern Natural unsubscribed page did not yield parseable rows: %s",
                response.url,
            )
