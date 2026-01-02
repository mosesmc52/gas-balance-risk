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
