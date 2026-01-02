# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NoticeItem(scrapy.Item):
    name = scrapy.Field()
    tsp = scrapy.Field()
    service_provider = scrapy.Field()
    url = scrapy.Field()
    critical = scrapy.Field()
    response = scrapy.Field()
    type = scrapy.Field()
    notice_id = scrapy.Field()
    prior_id = scrapy.Field()
    status = scrapy.Field()
    subject = scrapy.Field()
    body = scrapy.Field()
    kind = scrapy.Field()
    effective_dt = scrapy.Field()
    end_dt = scrapy.Field()
    posted_dt = scrapy.Field()
    response_dt = scrapy.Field()
    contact_phone = scrapy.Field()
    reason = scrapy.Field()
    location = scrapy.Field()
    published_by_email = scrapy.Field()
    file = scrapy.Field()


class CapacityItem(scrapy.Item):
    # Optional metadata
    source_url = scrapy.Field()
    downloaded_at_utc = scrapy.Field()

    # CSV fields (one row)
    Cycle_Desc = scrapy.Field()
    Post_Date = scrapy.Field()
    Eff_Gas_Day = scrapy.Field()
    Cap_Type_Desc = scrapy.Field()
    Post_Time = scrapy.Field()
    Eff_Time = scrapy.Field()
    Loc = scrapy.Field()
    Loc_Name = scrapy.Field()
    Loc_Zn = scrapy.Field()
    Flow_Ind_Desc = scrapy.Field()
    Loc_Purp_Desc = scrapy.Field()
    Loc_QTI_Desc = scrapy.Field()
    Meas_Basis_Desc = scrapy.Field()
    IT = scrapy.Field()
    All_Qty_Avail = scrapy.Field()
    Total_Design_Capacity = scrapy.Field()
    Operating_Capacity = scrapy.Field()
    Total_Scheduled_Quantity = scrapy.Field()
    Operationally_Available_Capacity = scrapy.Field()
    TSP_Name = scrapy.Field()
    TSP = scrapy.Field()
