FROM python:3.9

RUN apt-get update -y && apt-get install -y nano
RUN pip install numpy scipy xarray netCDF4 pymongo geopy

WORKDIR /app
COPY parse.py /app/parse.py
COPY populate.sh /app/populate.sh
COPY summaries.py /app/summaries.py
COPY data/basinmask_01.nc /app/data/basinmask_01.nc
