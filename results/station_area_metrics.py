from __future__ import division
import os
import sys
import pandas as pd
import argparse
from pandana.network import Network
from urbansim.utils import misc

# Initial options
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def metrics(net, hdf, stations, alt_number, dist=805, out='station_area_results.csv'):

    with pd.HDFStore(net) as store:
        print 'Loading {}'.format(net)
        edges = store.edges
        nodes = store.nodes

    with pd.HDFStore(hdf) as store:
        print 'Loading {}'.format(hdf)
        households = store.households
        parcels = store.parcels
        buildings = store.buildings
        jobs = store.jobs

    print 'Loading {}'.format(stations)
    stations = pd.read_csv(stations)

    net = Network(node_x=nodes['x'],
                  node_y=nodes['y'],
                  edge_from=edges['from'],
                  edge_to=edges['to'],
                  edge_weights=edges[['weight']])

    # Load stations and add first row placeholder
    alt = stations[stations.alternative == alt_number].set_index('station')
    alt.loc['placeholder'] = [1, -122.0, 37.0]
    alt = alt.sort('y')
    print 'Stations:'
    print alt

    # Set POIs in pandana network and do nearest neighbor analysis
    parcels['node_id'] = net.get_node_ids(parcels.x, parcels.y)
    net.init_pois(num_categories=1, max_dist=805.0, max_pois=1)
    net.set_pois("tmp", alt.x, alt.y)
    nearest = net.nearest_pois(dist, "tmp", num_pois=1, include_poi_ids=True)
    nearest.columns = ['dist', 'station']

    # Join to parcels and filter for those within half mile
    parcels_stations = parcels.merge(nearest, how='left', left_on='node_id', right_index=True)
    parcels_stations = parcels_stations[parcels_stations.station.isnull() == False]

    # Join stations to households and jobs
    buildings['station'] = misc.reindex(parcels_stations.station, buildings.parcel_id)
    households['station'] = misc.reindex(buildings['station'], households.building_id)
    jobs['station'] = misc.reindex(buildings['station'], jobs.building_id)

    # Filter for those within station buffer
    buildings_stations = buildings[buildings.station.isnull() == False]
    households_stations = households[households.station.isnull() == False]
    jobs_stations = jobs[jobs.station.isnull() == False]

    # Get dataframe of metrics by station area
    for index, series in alt.iterrows():

        hh = households_stations[households_stations.station == index]
        j = jobs_stations[jobs_stations.station == index]
        p = parcels_stations[parcels_stations.station == index]
        b = buildings_stations[buildings_stations.station == index]

        alt.loc[index, 'population'] = hh.persons.sum()
        alt.loc[index, 'households'] = len(hh)
        alt.loc[index, 'jobs'] = len(j)
        alt.loc[index, 'income_median'] = hh.income.median()

        for i in range(1, 5):

            alt.loc[index, 'income_quartile{}_count'.format(i)] = len(hh[hh.income_quartile == i])
            alt.loc[index, 'income_quartile{}_pct'.format(i)] = len(hh[hh.income_quartile == i]) / len(hh)

        alt.loc[index, 'res_units'] = b.residential_units.sum()
        alt.loc[index, 'nonres_sqft'] = b.non_residential_sqft.sum()

        alt.loc[index, 'soft_site_count'] = len(p[p.zoned_du_underbuild >= 1])
        alt.loc[index, 'soft_site_pct'] = len(p[p.zoned_du_underbuild >= 1]) / len(p)

    alt.to_csv(out)

if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--hdf', type=str, help='HDF5 file with households, parcels, buildings, and jobs tables')
    parser.add_argument('--net', type=str, help='HDF5 file with edges and nodes tables')
    parser.add_argument('--stations', type=str, help='CSV file with stations')
    parser.add_argument('--alt', type=int, help='Which alternative to use stations for')
    parser.add_argument('--dist', type=int, help='Distance in meters to perform nearest neighbor within')
    parser.add_argument('--out', type=str, help='Filepath to save csv to')
    args = parser.parse_args()

    # Define defaults and modify with command line arguments
    dist = 805
    out = 'station_area_results.csv'
    if args.dist:
        dist = args.dist
    if args.out:
        out = args.out

    metrics(args.net, args.hdf, args.stations, args.alt, dist, out)