from __future__ import division
import os
import sys
import pandas as pd
import argparse
from pandana.network import Network
from urbansim.utils import misc

# Initial options
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def metrics(net, hdf, stations, scenario, alternative, dist=805, out='city_results.csv'):

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

    cities = ['San Francisco', 'Oakland', 'Emeryville', 'Berkeley', 'Richmond', 'Walnut Creek']
    df = pd.DataFrame(index=cities)
    df['modeled_scenario'] = scenario
    df['modeled_alt'] = alternative



    alt = stations.set_index('station')

    net = Network(node_x=nodes['x'],
                  node_y=nodes['y'],
                  edge_from=edges['from'],
                  edge_to=edges['to'],
                  edge_weights=edges[['weight']])

    # Set POIs in pandana network and do nearest neighbor analysis
    parcels['node_id'] = net.get_node_ids(parcels.x, parcels.y)
    net.init_pois(num_categories=1, max_dist=805.0, max_pois=1)
    net.set_pois("tmp", alt.x, alt.y)
    nearest = net.nearest_pois(dist, "tmp", num_pois=1, include_poi_ids=True)
    nearest.columns = ['dist', 'station']

    # Join to parcels and filter for those within half mile
    parcels_stations = parcels.merge(nearest, how='left', left_on='node_id', right_index=True)
    parcels_stations = parcels_stations[~parcels_stations.station.isnull()]

    # Filter parcels for those within cities
    parcels_cities = parcels[parcels.juris_name.isin(cities)]

    # Join stations to households and jobs
    buildings['juris_name'] = misc.reindex(parcels_cities.juris_name, buildings.parcel_id)
    households['juris_name'] = misc.reindex(buildings['juris_name'], households.building_id)
    jobs['juris_name'] = misc.reindex(buildings['juris_name'], jobs.building_id)

    # Filter for those within station buffer
    buildings_cities = buildings[~buildings.juris_name.isnull()]
    households_cities = households[~households.juris_name.isnull()]
    jobs_cities = jobs[~jobs.juris_name.isnull()]

    # Calculate income_quartile if nonexistent (i.e. in baseline data)
    if 'income_quartile' not in households_cities.columns:
        s = pd.Series(pd.qcut(households_cities.income, 4, labels=False), index=households_cities.index)
        # convert income quartile from 0-3 to 1-4
        s = s.add(1)
        households_cities['income_quartile'] = s

    baseline = True if '2015_09_01_bayarea_v3' in hdf else False

    # Get dataframe of metrics by city
    for index, series in df.iterrows():

        hh = households_cities[households_cities.juris_name == index]
        j = jobs_cities[jobs_cities.juris_name == index]
        p = parcels_cities[parcels_cities.juris_name == index]
        b = buildings_cities[buildings_cities.juris_name == index]

        df.loc[index, 'population'] = hh.persons.sum()
        df.loc[index, 'households'] = len(hh)
        df.loc[index, 'jobs'] = len(j)
        df.loc[index, 'income_median'] = hh.income.median()

        for i in range(1, 5):
            df.loc[index, 'income_quartile{}_count'.format(i)] = len(hh[hh.income_quartile == i])

            try:
                df.loc[index, 'income_quartile{}_pct'.format(i)] = len(hh[hh.income_quartile == i]) / len(hh)
            except ZeroDivisionError:
                df.loc[index, 'income_quartile{}_pct'.format(i)] = 0

        df.loc[index, 'res_units'] = b.residential_units.sum()
        df.loc[index, 'nonres_sqft'] = b.non_residential_sqft.sum()

        if not baseline:
            df.loc[index, 'soft_site_count'] = len(p[p.zoned_du_underbuild >= 1])

            try:
                df.loc[index, 'soft_site_pct'] = len(p[p.zoned_du_underbuild >= 1]) / len(p)
            except ZeroDivisionError:
                df.loc[index, 'soft_site_pct'] = 0

    df.to_csv(out)

if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--hdf', type=str, help='HDF5 file with households, parcels, buildings, and jobs tables')
    parser.add_argument('--net', type=str, help='HDF5 file with edges and nodes tables')
    parser.add_argument('--stations', type=str, help='CSV file with stations')
    parser.add_argument('--scen', type=str, help='Which scenario')
    parser.add_argument('--alt', type=int, help='Which alternative')
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

    metrics(net=args.net,
            hdf=args.hdf,
            stations=args.stations,
            scenario=args.scen,
            alternative=args.alt,
            dist=dist,
            out=out)
