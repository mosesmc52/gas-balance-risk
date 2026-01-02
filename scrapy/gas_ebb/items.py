# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Notice(scrapy.Item):
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


class OperationAvailableCapacity(scrapy.Item):
    tsp_name = scrapy.Field()
    tsp_id = scrapy.Field()
    url = scrapy.Field()
    posted_dt = scrapy.Field()
    effective_dt = scrapy.Field()
    type = scrapy.Field()
    location_name = scrapy.Field()
    location_id = scrapy.Field()
    location_zone = scrapy.Field()
    location_purpose_description = scrapy.Field()
    location_quantity_type_indicator = scrapy.Field()
    flow_indicator = scrapy.Field()
    all_quantity_available = scrapy.Field()
    design_capacity = scrapy.Field()
    operating_capacity = scrapy.Field()
    total_scheduled_quantity = scrapy.Field()
    operationally_available_capacity = scrapy.Field()
    interruptible_transportation_indicator = scrapy.Field()
    quantity_reason = scrapy.Field()


class UnsubscribedCapacity(scrapy.Item):
    tsp_name = scrapy.Field()
    tsp_id = scrapy.Field()
    url = scrapy.Field()
    posted_dt = scrapy.Field()
    effective_dt = scrapy.Field()
    type = scrapy.Field()
    location_name = scrapy.Field()
    location_id = scrapy.Field()
    location_zone = scrapy.Field()
    location_purpose_description = scrapy.Field()
    location_quantity_type_indicator = scrapy.Field()
    design_capacity = scrapy.Field()
    unsubscribed_capacity = scrapy.Field()


class StationWeatherFileItem(scrapy.Item):
    """
    Emitted once per station download.
    """

    pipeline = scrapy.Field()  # e.g., "algonquin"
    ghcnd_station_id = scrapy.Field()  # e.g., "USW00014739"
    source_url = scrapy.Field()  # NOAA access URL for station
    local_path = scrapy.Field()  # where file was written
    http_status = scrapy.Field()  # response status
    fetched_at_utc = scrapy.Field()  # ISO timestamp
    bytes_written = scrapy.Field()  # int
    note = scrapy.Field()  # optional message


class RegionDailyWeatherItem(scrapy.Item):
    """
    Emitted when the regional daily aggregation is produced.
    This is typically yielded once on spider close (for the pipeline).
    """

    pipeline = scrapy.Field()  # "algonquin"
    local_path = scrapy.Field()  # where regional file was written
    start_date = scrapy.Field()  # YYYY-MM-DD
    end_date = scrapy.Field()  # YYYY-MM-DD
    n_days = scrapy.Field()  # int
    stations_used = scrapy.Field()  # int
    aggregation_method = scrapy.Field()  # e.g., "median(TAVG_C)"
    base_temp_f = scrapy.Field()  # e.g., 65
    note = scrapy.Field()
