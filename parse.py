# usage: python parse.py <argo BGC+ netcdf file>
import xarray, math, datetime, sys
from pymongo import MongoClient
from geopy import distance

client = MongoClient('mongodb://database/argo')
db = client.argo

def parse_location(longitude, latitude, suppress=True):
    # given the raw longitude, latitude from a netcdf file,
    # normalize, clean and log problems

    # official fill value from https://archimer.ifremer.fr/doc/00187/29825/86414.pdf, followed by things seen in the wild
    latitude_fills = [99999, -99.999, -999.0]
    longitude_fills = [99999, -999.999, -999.0] 

    if math.isnan(latitude) or latitude in latitude_fills or math.isnan(longitude) or longitude in longitude_fills:
        if not suppress:
            print(f'warning: LONGITUDE={longitude}, LATITUDE={latitude}, setting to 0,-90')
        return 0, -90
    elif longitude < -180:
        if not suppress:
            print('warning: mutating longitude < -180')
        return longitude + 360, latitude
    elif longitude > 180:
        if not suppress:
            print('warning: mutating longitude > 180')
        return longitude - 360, latitude
    else:
        return longitude, latitude

def find_basin(lon, lat, basins, suppress=True):
    # for a given lon, lat,
    # identify the basin from the lookup table.
    # choose the nearest non-nan grid point.

    gridspacing = 0.5

    basin = basins['BASIN_TAG'].sel(LONGITUDE=lon, LATITUDE=lat, method="nearest").to_dict()['data']
    if math.isnan(basin):
        # nearest point was on land - find the nearest non nan instead.
        lonplus = math.ceil(lon / gridspacing)*gridspacing
        lonminus = math.floor(lon / gridspacing)*gridspacing
        latplus = math.ceil(lat / gridspacing)*gridspacing
        latminus = math.floor(lat / gridspacing)*gridspacing
        grids = [(basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonplus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonplus)).miles)]

        grids = [x for x in grids if not math.isnan(x[0])]
        if len(grids) == 0:
            # all points on land
            if not suppress:
                print('warning: all surrounding basin grid points are NaN')
            basin = -1
        else:
            grids.sort(key=lambda tup: tup[1])
            basin = grids[0][0]
    basins.close()
    return int(basin)

basins = xarray.open_dataset('data/basinmask_01.nc')

xar = xarray.open_dataset(sys.argv[1])

# construct a metadata document for this float
meta_doc ={
    '_id': xar['PLATFORM_NUMBER'].data[0].decode().strip() + '_m0',
    'data_type': 'expertQC_oceanicProfile',
    'data_center': xar['DATA_CENTRE'].data[0].decode().strip(),
    'instrument': 'profiling_float',
    'pi_name': [x.strip() for x in xar['PI_NAME'].data[0].decode().split(',')],
    'platform': xar['PLATFORM_NUMBER'].data[0].decode().strip(),
    'platform_type': xar['PLATFORM_TYPE'].data[0].decode().strip(),
    'positioning_system': xar['POSITIONING_SYSTEM'].data[0].decode().strip(),
    'wmo_inst_type': str(xar['WMO_INST_TYPE'].data[0].decode().strip()),

}
meta_doc['fleetmonitoring'] = 'https://fleetmonitoring.euro-argo.eu/float/' + str(meta_doc['platform'])
meta_doc['oceanops'] = 'https://www.ocean-ops.org/board/wa/Platform?ref=' + str(meta_doc['platform'])

try:
    db.bgcargoplusMeta.replace_one({'_id': meta_doc['_id']}, meta_doc, True)
except BaseException as err:
    print('error: data upsert failure on', meta_doc)
    print(err)

# construct a data document for each profile
nprof = len(xar.coords['N_PROF'].data)
for i in range(nprof):
    data_doc = {}
    floatnumber = xar['PLATFORM_NUMBER'].data[i].decode().strip()
    cycle = str(int(xar['CYCLE_NUMBER'].data[i]))
    direction = xar['DIRECTION'].data[i].decode()
    id = f"{floatnumber}_{cycle.zfill(3)}"
    if direction == 'D':
        id = f"{id}D"
    longitude, latitude = parse_location(xar['LONGITUDE'].data[i], xar['LATITUDE'].data[i])
    juld = xar['JULD'].data[i]

    data_doc['_id'] = id
    data_doc['geolocation'] = {"type": "Point", "coordinates": [float(longitude), float(latitude)]}
    data_doc['basin'] = find_basin(longitude, latitude, basins)
    try:
        data_doc['timestamp'] = datetime.datetime.utcfromtimestamp(juld.astype('datetime64[ms]').astype(int)/1000)
    except:
        data_doc['timestamp'] = None
    data_doc['date_updated_argovis'] = datetime.datetime.now()
    data_doc['source'] = [{'source': ['bgcargo+'], 'doi': 'tbd'}]
    data_doc['data_info'] = [[],['units', 'mode'],[]]
    data_doc['cycle_number'] = int(xar['CYCLE_NUMBER'].data[i])
    try:
        data_doc['geolocation_argoqc'] = int(xar['POSITION_QC'].data[i].decode())
    except:
        data_doc['geolocation_argoqc'] = -1
    data_doc['profile_direction'] = direction
    try:
        data_doc['timestamp_argoqc'] = int(xar['JULD_QC'].data[i].decode())
    except:
        data_doc['timestamp_argoqc'] = -1
    data_doc['metadata'] = [meta_doc['_id']]

    data_doc['data'] = []

    parameter = xar['PARAMETER'].data[i]
    for vix, var in enumerate(parameter[0]):
        varname = var.decode().strip() + '_ADJUSTED_RO'
        if varname == '_ADJUSTED_RO':
            # some blank entrues in PARAMETER, skip
            continue
        data = xar[varname].data[0]
        parameter_data_mode = xar['PARAMETER_DATA_MODE'].data[i][vix].decode()
        unit = xar[varname].attrs['units']
        data = xar[varname].data[0]
        data_doc['data'].append([float(x) for x in list(data)])
        # Argovis API requires something be named exactly 'pressure'
        name = varname
        if name == 'PRES_ADJUSTED_RO':
            name = 'pressure'
        data_doc['data_info'][0].append(name)
        data_doc['data_info'][2].append([unit, parameter_data_mode])

    # must have a pressure axis
    if 'pressure' not in data_doc['data_info'][0]:
        print('error: no pressure axis found, skipping', id)
        print(xar['PARAMETER'].data[i])
        continue

    try:
        db.bgcargoplus.replace_one({'_id': data_doc['_id']}, data_doc, True)
    except BaseException as err:
        print('error: data upsert failure on', data_doc)
        print(err)
