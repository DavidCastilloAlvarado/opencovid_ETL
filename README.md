# ETL by Django Command

## **Installation:**

1.  Clone this repo

        $git clone https://github.com/DavidCastilloAlvarado/opencovid_ETL.git

        $cd opencovid_ETL

2.  Install and active the environment (power shell)

        $python -m pip install --user virtualenv
        $python -m venv .
        $source ./Scripts/activate

3.  Download all required libraries

        $pip install -r requirements.txt

## **Quickstart**

1. Setup your own json apikey on API folder

2. Set your own database credential Postgresql

In `etldjango/settings.py`:

```python

    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': 'opencoviddb',
        'USER': 'datacrew',
        'PASSWORD': 'admin1234',
        'HOST': '127.0.0.1',
        'DATABASE_PORT': ' 5432',
        'TEST': {
            'NAME': 'mytestdatabase',
        },

    },
```

2.  Make all migrations before to run

        $python manage.py makemigrations
        $python manage.py migrate

## **Using**

### _Management commands_

All the commands could be executed independently in any time. They download its own data from the bucket. Before run any command the first command has to be executed to have data in the bucket.

1. Download all the raw data from the gobernment

```bash
python manage.py worker_extractor

```

2. Calculate Movility - center roller mean 7 days

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

5. Command for calculate RT score

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
python manage.py worker_t_caphosp full
# to update the last values
python manage.py worker_t_caphosp last
```

7.  Command for Sinadef table report - center roller mean 7 days

```bash
# To initialize the data set
python manage.py worker_sinadef full
# to update the last values
python manage.py worker_sinadef last
```

8. Command for UCI status and geopoints

```bash
# upload the whole day status and delete the rest
python manage.py worker_t_uci_geo full
# append the last day calculations in the current table
python manage.py worker_t_uci_geo last
```

9. Command for OXI status table by regions

```bash
# upload the whole day status and delete the rest
python manage.py worker_t_oxistat full
# append the last day calculations in the current table
python manage.py worker_t_oxistat last
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
