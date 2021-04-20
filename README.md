# ETL by Django Command

### Python 3.6.9

## **Installation:**

1.  Clone this repo

```bash
git clone https://github.com/DavidCastilloAlvarado/opencovid_ETL.git

cd opencovid_ETL
```

2.  Install and active the environment

```bash
python -m pip install --user virtualenv
python -m venv .
source ./bin/activate #./Scripts/activate
```

3.  Download all required libraries

```bash
pip install -r requirements.txt
```

## **Quickstart**

1. Setup your own json apikey on API folder

2. Set your own credential for postgres and GCP services in your enviorment .env

```bash
IP_SERVER=************  #postgres
PASSWORD=************   #postgres
NAME=************       #database name
USER_NAME=************  #postgres username
GCP_PROJECT_ID=************
BUCKET_NAME=************
BUCKET_ROOT=************ # folder in your bucket
KEY_JSON_FILE=************.json
KEY_MAPS_API =************ # Key googlemaps api
```

2.  Make all migrations before to run

```bash
cd etldjango
python manage.py makemigrations
python manage.py migrate
```

## **Using**

### _Management commands_

1. Initializing our bucket container - run it just one time

```bash
python manage.py worker_init_bucket
```

1.1 Update all reports to initialize - run it just one time

```bash
python manage.py worker_extractor v1
python manage.py worker_update_all_init full
```

1.2 Update all reports - **daily update** <- run it every day or regularly to update everything.

```bash
python manage.py worker_extractor v2
python manage.py worker_update_all last
```

# Individual updates

2. Calculate Movility

```bash
# To initialize the data set
python manage.py worker_mov full

# to update the last values
python manage.py worker_mov last

```

3. Command for only positive cases table from MINSA dataset

```bash
# To initialize the data set
python manage.py post_rel full
# to update the last values
python manage.py post_rel last

```

4. Command for positive/test cumulative cases table from PDF daily MINSA report - % positivity

```bash

# for upload the data from a csv in the bucket
python manage.py worker_posit csv
# for AUTO update the data from the last record until today
python manage.py workrt_posit pdf --update yes
# load the last pdf from minsa report
python manage.py worker_posit pdf
# load a particular daily report from minsa %d%m%y
python manage.py worker_posit pdf --day 230321
```

5. Command for calculate RT score (WARNING: the calculation could be wrong, run it manually)

```bash
# reboot the db for the last 12months
python manage.py worker_rt full
# reboot the db for the last 6 months
python manage.py worker_rt full --m 6
# update the db using the last month
python manage.py worker_rt last
# update the db using the las 6 months
python manage.py worker_rt last --m 6
```

6. Command for calculate hospital capacity

```bash
# To initialize the data set
python manage.py worker_t_caphospv2 full
# to update the last values
python manage.py worker_t_caphospv2 last
```

7.  Command for Sinadef table report - roller mean 7 days

```bash
# To initialize the data set
python manage.py worker_sinadef full
# to update the last values
python manage.py worker_sinadef last
```

8. Command for UCI status and geopoints

```bash
# upload the whole day status and delete the rest
python manage.py worker_t_uci_geov2 full
# append the last day calculations in the current table
python manage.py worker_t_uci_geov2 last
```

9. Command for OXI status table by regions

```bash
# upload the whole day status and delete the rest
python manage.py worker_t_oxistat full
# append the last day calculations in the current table
python manage.py worker_t_oxistatv2 last
```

10. Command for the Vaccine resume table

```bash
# upload the whole day status and delete the rest
python manage.py worker_t_vacunas full
# append the last day calculations in the current table
python manage.py worker_t_vacunas last
```

11. Command for the epidemiological ranking table

```bash
# initialize the database using the last 3 weeks
python manage.py worker_t_epidem full --w 3
# append the last three weeks to the database
python manage.py worker_t_epidem last --w 3
```

12. Command for the daily record table for positive and test cases and roller mean.

```bash
# reboot the database for the last 12months
python manage.py worker_positividad full
# update the database using the last 3 months
python manage.py worker_positividad last --m 3
```

13. Command for update the deaths table from Minsa dataset

```bash
# initialize the database
python manage.py worker_t_minsamuertes full
# append the last data only
python manage.py worker_t_minsamuertes last
```

14. Command for update the oxi provider table

```bash
# Search providers in every local area (high cost in cloud services)
python manage.py worker_oxi_provider search
# Load data from a data set in csv format
python manage.py worker_oxi_provider csv

```

15. Command for update the resumen table.

```bash
python manage.py worker_t_resumen

```
