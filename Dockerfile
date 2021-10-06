FROM python:3.6.9-slim
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
RUN apt-get update && apt-get install -y  libmariadb-dev-compat libmariadb-dev \
    build-essential libssl-dev libffi-dev \
    libxml2-dev libxslt1-dev zlib1g-dev 
   
RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

COPY . /usr/src/app/
CMD python3 mqtt_parser.py

# ADD oracle_deb/oracle-instantclient-basic_21.1.0.0.0-2_amd64.deb /usr/src/app

# #if libaio also required
# RUN apt-get install libaio1 
# RUN dpkg -i oracle-instantclient-basic_21.1.0.0.0-2_amd64.deb 

# # copy project
# COPY . /usr/src/app/

#copy entrypoint.sh
# COPY ./entrypoint.sh /usr/src/app/entrypoint.sh

# ENTRYPOINT ["/usr/src/app/entrypoint.sh"]