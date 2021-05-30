# pull official base image
FROM python:3.7
#EXPOSE 9002
# set work directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# update pip
RUN apt-get update
RUN pip install --upgrade pip

# Install geolibs for geodjango
RUN apt-get install -y\
    binutils \ 
    libproj-dev \ 
    gdal-bin \
    autoconf \
    automake \
    libtool \
    curl \ 
    make \
    g++ \
    unzip \
    pkg-config. \
    libpq-dev \
    libxml2-dev \
    sqlite3 \
    libsqlite3-dev
#RUN apt install postgresql-server-dev-11
# Setup GDAL
RUN apt-get install -y binutils \
    libproj-dev \
    gdal-bin\
    python-gdal\
    python3-gdal


# #Installing GEOS
# RUN wget https://download.osgeo.org/geos/geos-3.8.1.tar.bz2
# RUN tar xjf geos-3.8.1.tar.bz2
# RUN ./geos-3.8.1/configure
# RUN cd geos-3.8.1
# RUN make
# RUN make install
# RUN cd ..

# #Installing PROJ4
# RUN wget https://download.osgeo.org/proj/proj-6.3.1.tar.gz
# RUN wget https://download.osgeo.org/proj/proj-datumgrid-1.8.tar.gz

# RUN tar xzf proj-6.3.1.tar.gz
# RUN cd proj-6.3.1/data
# RUN tar xzf ../../proj-datumgrid-1.8.tar.gz
# RUN cd ..

# RUN ./configure
# RUN make -j8
# RUN make install
# RUN cd ..

# ## Install json-c
# RUN wget https://github.com/json-c/json-c/archive/refs/tags/json-c-0.15-20200726.tar.gz
# RUN tar -xvzf json-c-0.15-20200726.tar.gz
# RUN mv json-c-json-c-0.15-20200726 json-c
# RUN cd json-c
# RUN mkdir json-c-build
# RUN cmake ../json-c
# RUN make
# RUN make install
# RUN cd ..

# # Installing GDAL
# RUN wget https://download.osgeo.org/gdal/2.4.4/gdal-2.4.4.tar.gz
# RUN tar xzf gdal-2.4.4.tar.gz
# RUN cd gdal-2.4.4
# RUN ./configure --with-python
# RUN make -j8
# RUN make install
# RUN cd ..


# Instaling dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt


# copy project
COPY . /usr/src/app/

# run development server
#CMD python /usr/src/app/manage.py runserver 0.0.0.0:$PORT

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 etldjango.wsgi:application
