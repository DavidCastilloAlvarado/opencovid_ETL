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

```shell

python manage.py worker_extractor
python manage.py worker_getbucket
python manage.py worker_t_sinadef no
python manage.py worker_t_uci yes
python manage.py worker_posit no
python manage.py worker_rt db
python manage.py worker_mov

```
