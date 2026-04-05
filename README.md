# gas-balance-risk
Probabilistic monitoring of regional gas shortfall risk.

## DigitalOcean Functions

The scheduled job launcher lives in [`infra/do-functions`](/Users/mozilla/Documents/projects/gas-balance-risk/infra/do-functions).

### Required setup

1. Copy `infra/do-functions/.env.example` to `infra/do-functions/.env`.
2. Fill in the required secrets and deployment values.
3. Use the Mongo droplet private VPC address in `MONGO_URI`, not the public IP.
4. Set `DO_VPC_UUID` so the launched job droplet is created in the same VPC as Mongo.
5. Keep `DO_APP_TAG=app-runners` so the Mongo firewall rule allows port `27017`.

Example Mongo connection:

```dotenv
MONGO_URI=mongodb://admin:admin123@10.108.0.4:27017/?authSource=admin
```

### Deploy the function

Validate the project metadata:

```bash
make do-fn-validate
```

Connect `doctl` to the target Functions namespace:

```bash
make do-fn-connect
```

Deploy:

```bash
make do-fn-deploy
```

Remote build variant:

```bash
make do-fn-deploy-remote
```

Invoke manually:

```bash
make do-fn-invoke
```

List recent activations:

```bash
make do-fn-activations
```

View a function activation log:

```bash
make do-fn-logs ACTIVATION=<activation-id>
```

### View the launched droplet logs

The function logs only show the launcher action. The actual workload runs on the spawned droplet.

Tail the droplet job log over SSH:

```bash
make do-droplet-log DROPLET_IP=<job-droplet-ip>
```

This tails:

```bash
sudo tail -f /var/log/job.log
```

Useful early boot log:

```bash
ssh root@<job-droplet-ip> "sudo tail -f /var/log/cloud-init-output.log"
```

If the droplet already shut down and uploaded logs to Spaces, print the uploaded log:

```bash
make do-spaces-log LOG_KEY=logs/gas-risk-daily/YYYY/MM/DD/<file>.log
```

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
