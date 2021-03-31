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
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'opencovidlocal',
        'USER': 'postgres',
        'PASSWORD': 'admin1234',
        'HOST': '127.0.0.1',
        'DATABASE_PORT': '5432',
    }
```

2.  Make all migrations before to run

        $python manage.py makemigrations
        $python manage.py migrate

## **Using**

### _Management command_

All the commands could be executed independently in any time. They download its own data from the bucket. Previously to run any command the first command has to be executed to have data to process.

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

3. Table for positive cases from MINSA dataset

```bash
# To initialize the data set
python manage.py post_rel full
# to update the last values
python manage.py post_rel last

```

4. Table for positive cases from daily MINSA report - % positivity

```bash

python manage.py worker_posit

```

5. Calculate RT score

```bash
# To initialize the data set
python manage.py worker_rt full
# to update the last values
python manage.py worker_rt last
```

6. Calculate hospital capacity

```bash
# To initialize the data set
python manage.py worker_t_caphosp full
# to update the last values
python manage.py worker_t_caphosp last
```

7. Sinadef table report

```bash
# To initialize the data set
python manage.py worker_sinadef full
# to update the last values
python manage.py worker_sinadef last
```

8. Table for UCI status and geopoinst

```bash
# upload the whole day status and delete the rest
python manage.py worker_t_uci_geo full
# append the last day calculations in the current table
python manage.py worker_t_uci_geo last
```
