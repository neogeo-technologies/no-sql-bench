FROM python:2.7
MAINTAINER Neogeo Technologies http://www.neogeo-online.net

# Install client libs for PostgreSQL
RUN apt-get update && apt-get install -y libpq-dev nano

# Install python modules required by the data integration script
RUN pip install --upgrade pip
RUN pip install \
	requests \
	scriptine \
	psycopg2 \
	CouchDB \
	pycouchdb \
	pymongo \
	python-dateutil \
	pyyaml

# Copy the scripts
COPY scripts /poc_nosql/scripts
RUN chmod +x /poc_nosql/scripts/*.py