import pandas as pd
import argparse
from urbansim.utils import misc

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--hdf', type=str, help='HDF5 file to load')
parser.add_argument('--co', type=str, help='Connect Oakland parcels csv file')
args = parser.parse_args()

with pd.HDFStore(args.hdf) as store:
    parcels = store.parcels
    buildings = store.buildings
    households = store.households
    jobs = store.jobs

co = pd.read_csv(args.co)
co_parcel_ids = co.parcel_id.tolist()

households['parcel_id'] = misc.reindex(buildings['parcel_id'], households.building_id)
jobs['parcel_id'] = misc.reindex(buildings['parcel_id'], jobs.building_id)

co_hh = households.loc[households.parcel_id.isin(co_parcel_ids)]
co_jobs = jobs.loc[jobs.parcel_id.isin(co_parcel_ids)]
co_buildings = buildings.loc[buildings.parcel_id.isin(co_parcel_ids)]

co_hh.to_csv('co_hh.csv', index=False)
co_jobs.to_csv('co_jobs.csv', index=False)
co_buildings.to_csv('co_buildings.csv', index=False)
