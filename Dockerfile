FROM python:3.9

RUN apt-get update -y && apt-get install -y nano
RUN pip install numpy scipy xarray netCDF4 pymongo
