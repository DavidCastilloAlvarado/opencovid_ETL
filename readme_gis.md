# GEO DJANGO [aqui](https://docs.djangoproject.com/en/3.1/ref/contrib/gis/install/geolibs/)

# How to initi postgresql

# Install postgress [aqui](https://www.postgresql.org/download/linux/ubuntu/)

```bash
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install postgresql-11
```

#### how to remove postgress [aqui](https://askubuntu.com/questions/32730/how-to-remove-postgres-from-my-installation#:~:text=One%20command%20to%20completely%20remove,postgresql%20and%20all%20it's%20compenents.)

#### see all packege to remove

```
dpkg -l | grep postgres
```

#### To remove postgres

```
sudo apt-get --purge remove postgresql\*
```

# Test postgress conection

```bash
sudo systemctl is-active postgresql
sudo systemctl is-enabled postgresql
sudo systemctl status postgresql

sudo pg_isready
```

# Creating database in postgresql

```
sudo su - postgres
psql
postgres=# CREATE USER datacrew WITH PASSWORD 'admin1234';
postgres=# CREATE DATABASE opencoviddb;
postgres=# GRANT ALL PRIVILEGES ON DATABASE opencoviddb to datacrew;
postgres=# \q
```

# Config authentication

##### sudo nano /etc/postgresql/12/main/pg_hba.conf

# After config, restart

```
sudo systemctl restart postgresql
```

# Start the instalation of geo django

### Installing Geospatial libraries

```bash
sudo apt-get install binutils libproj-dev gdal-bin
```

## Install some dependencies

```bash
sudo apt-get install autoconf automake libtool curl make g++ unzip
sudo apt-get install -y pkg-config.
sudo apt-get install libpq-dev
sudo apt-get install libxml2-dev
sudo apt install postgresql-server-dev-11
sudo apt-get install sqlite3 libsqlite3-dev
```

## GEOS see versions [here](https://trac.osgeo.org/geos) 3.8, 3.7, 3.6, 3.5

```bash
wget https://download.osgeo.org/geos/geos-3.8.1.tar.bz2
tar xjf geos-3.8.1.tar.bz2
cd geos-X.Y.Z
./configure
make
sudo make install
cd ..
```

## PROJ.4 6.3, 6.2, 6.1, 6.0, 5.x, 4.x [here](https://proj.org/download.html#current-release)

```bash
wget https://download.osgeo.org/proj/proj-6.3.1.tar.gz
wget https://download.osgeo.org/proj/proj-datumgrid-1.8.tar.gz

tar xzf proj-6.3.1.tar.gz
cd proj-6.3.1/data
tar xzf ../../proj-datumgrid-1.8.tar.gz
cd ..

./configure
make -j8
sudo make install
cd ..
```

## Install json-c

```bash
wget https://github.com/json-c/json-c/archive/refs/tags/json-c-0.15-20200726.tar.gz
tar -xvzf json-c-0.15-20200726.tar.gz
mv json-c-json-c-0.15-20200726 json-c
cd json-c
mkdir json-c-build
cmake ../json-c
make
sudo make install
cd ..
```

### Install GDAL [aqui](https://gdal.org/download.html)

```bash
wget https://download.osgeo.org/gdal/2.4.4/gdal-2.4.4.tar.gz
tar xzf gdal-2.4.4.tar.gz
cd gdal-2.4.4
./configure --with-python
make -j8
sudo make install
cd ..
```

## Install PostGIS [aqui](http://postgis.net/stuff/) [o aquí](https://postgis.net/docs/postgis_installation.html)

#install_requirements

https://trac.osgeo.org/postgis/wiki/UsersWikiPostgreSQLPostGIS

```bash
wget http://postgis.net/stuff/postgis-2.5.4.tar.gz
tar -xvzf postgis-2.5.4.tar.gz
cd postgis-2.5.4
./configure --without-protobuf-c
make -j8
make check
sudo make install
cd ..
```

##### add into database

```
sudo su - postgres
psql
CREATE EXTENSION postgis;
CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION postgis_tiger_geocoder;
```

## Install SpatiaLite

```bash
sudo apt-get install libsqlite3-mod-spatialite
```
