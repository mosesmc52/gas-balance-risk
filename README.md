# gas-balance-risk
Probabilistic monitoring of regional gas shortfall risk.


To run Algonquin Notices Crawler
```
    poetry run scrapy crawl algonquin_notices
```

# Tasks
    - Store Data in Mongo
    - Store Json data in Digital Ocean Storage
    - Download EIA data - Needed
        Storage - https://www.eia.gov/naturalgas/storage/
        Henry Hub spot price - https://www.eia.gov/dnav/ng/hist/rngwhhdD.htm
        Optional
        Natural Gas Regional Prices - https://www.eia.gov/dnav/ng/ng_pri_sum_dcu_nus_a.htm
    - Downlaod NOAA data
        - Calculate Mean of Stations in cities in states that Algolquin locatd
        https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily

        https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/
    - Setup docker-compose to run everything scheduled daily
    - build PyMC model
    - Build AI reporting Agent
    - Automate Reporting Send Email, Use Google Sheets to store Email List
    - UI Build Dashboard


Tier 1 — Highest Priority (Expand here first)

These pipelines have frequent operational stress, price impact, and clear regional footprints.

1. Algonquin Gas Transmission x

    Region: New England

    Why it matters: Winter bottlenecks, LNG competition, extreme price spikes

    Status: Your current focus — excellent choice

2. Transcontinental Gas Pipe Line (Transco)

    Region: Southeast → Mid-Atlantic → Northeast

    Why it matters: Backbone of East Coast gas

    Stress drivers: Maintenance, power burn surges, hurricanes

    Expansion priority: #1 after Algonquin

3. Tennessee Gas Pipeline

    Region: Gulf → Midwest → Northeast

    Why it matters: Major Marcellus outlet

    Stress drivers: Compression outages, winter constraints

    Good for: Comparing Appalachian vs New England dynamics

Tier 2 — Regionally Critical (Strong expansion candidates)
4. Texas Eastern Transmission (Tetco)

    Region: Texas → Appalachia → Northeast

    Why it matters: Heavily interconnected

    Stress drivers: Maintenance cascades across regions

    Model strength: Excellent for network-stress effects

5. Columbia Gas Transmission
    https://ebb.tceconnects.com/infopost/TCeConnects.aspx?v=1.1&SID=67&info=Y&assetid=14
    Region: Appalachia → Midwest / Mid-Atlantic

    Why it matters: Dense industrial + power demand

    Stress drivers: Storage cycling, winter demand

6. El Paso Natural Gas

    Region: Permian → Southwest / California

    Why it matters: Southwest + CA reliability

    Stress drivers: Heat waves, drought, power burn

    Seasonality: Summer stress (complements winter pipelines)

    Tier 3 — Strategic / Advanced Coverage

    These are valuable once your platform is proven.

7. Northern Natural Gas

Region: Midwest

    Why it matters: Storage-heavy region

    Good for: Storage-driven stress modeling

8. ANR Pipeline
https://ebb.anrpl.com/
Region: Midwest

    Why it matters: Industrial and utility demand

    Stress drivers: Cold snaps, storage drawdowns

9. Kinder Morgan Louisiana Pipeline
https://pipeline2.kindermorgan.com/default.aspx?code=KMLP
Region: Gulf Coast

    Why it matters: LNG feedgas exposure

    Stress drivers: LNG ramp-ups, outages

    Commercial value: High trader interest

10. SoCal Gas Transmission
    https://www.socalgasenvoy.com/index.jsp#nav=/Public/ViewExternal.showHome
    Region: Southern California

    Why it matters: Chronic constraints

    Stress drivers: Regulatory limits, heat waves

    Note: More intrastate, but huge price impact
