import pandas as pd
import argparse
from urbansim.utils import misc

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--file', type=str, help='Filepath of parcel-level HDF5 file')
parser.add_argument('--newfile', type=str, help='New filepath')
args = parser.parse_args()

filepath = args.file
new = args.newfile

# Read parcel-to-block table
parcel_block = pd.read_csv('parcel_to_block.csv', dtype=str)
parcel_block = parcel_block.set_index('parcel_id')

# Read block geometry table and set block_id to index of blocks table
block_geom = pd.read_csv('block_geoms.csv', dtype='str')
blocks = pd.DataFrame(index=block_geom.block_id)

# Read HDF5 file of baseline data
with pd.HDFStore(filepath) as store:
    parcels = store.parcels
    buildings = store.buildings
    households = store.households
    jobs = store.jobs

store = pd.HDFStore('bay_area_block_model_data.h5', mode='r')

# Get block info to blocks table
blocks['square_meters_land'] = store.blocks.square_meters_land

# Get block_ids to parcels and buildings
parcels['block_id'] = misc.reindex(parcel_block.block_id, parcels.index.astype(str))
buildings['block_id'] = misc.reindex(parcel_block.block_id, buildings.parcel_id.astype(str))

# Get block_ids to jobs and households
jobs['block_id'] = misc.reindex(buildings.block_id, jobs.building_id)
households['block_id'] = misc.reindex(buildings.block_id, households.building_id)

new_store = pd.HDFStore(new, mode='w')
new_store['parcels'] = parcels
new_store['buildings'] = buildings
new_store['blocks'] = blocks
new_store['households'] = households
new_store['jobs'] = jobs
new_store['drive_edges'] = store.drive_edges
new_store['drive_nodes'] = store.drive_nodes

new_store.close()
store.close()
