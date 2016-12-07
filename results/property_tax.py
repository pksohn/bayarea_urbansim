from __future__ import division
import pandas as pd
import argparse
from pandana.network import Network
from urbansim.utils import misc

# Initial options
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def summary(hdf, out, alternative,
            counties=False, station_csv=None, net=None):

    baseline = True if '2015_09_01_bayarea_v3' in hdf else False

    with pd.HDFStore(hdf) as store:
        print 'Loading {}'.format(hdf)
        p = store.parcels
        b = store.buildings

    if station_csv:

        with pd.HDFStore(net) as store:
            print 'Loading {}'.format(net)
            edges = store.edges
            nodes = store.nodes

        print 'Loading {}'.format(station_csv)
        stations = pd.read_csv(station_csv)

        net = Network(node_x=nodes['x'],
                      node_y=nodes['y'],
                      edge_from=edges['from'],
                      edge_to=edges['to'],
                      edge_weights=edges[['weight']])

        alt = stations.set_index('station')
        alt['modeled_alt'] = alternative

        def station_in_modeled_alt(row):

            alt_num = row['modeled_alt']
            alt_col = 'alt{}'.format(alt_num)
            if alt_col in alt.columns:
                if row[alt_col] == 1:
                    return 1
                else:
                    return 0
            else:
                return 0

        alt['station_in_modeled_alt'] = alt.apply(station_in_modeled_alt, axis=1)

        alt = alt.loc['station_in_modeled_alt' == 1]

        # Set POIs in pandana network and do nearest neighbor analysis
        p['node_id'] = net.get_node_ids(p.x, p.y)
        net.init_pois(num_categories=1, max_dist=805.0, max_pois=1)
        net.set_pois("tmp", alt.x, alt.y)
        nearest = net.nearest_pois(806, "tmp", num_pois=1, include_poi_ids=True)
        nearest.columns = ['dist', 'station']

        # Join to parcels and filter for those within half mile
        parcels_stations = p.merge(nearest, how='left', left_on='node_id', right_index=True)
        parcels_stations = parcels_stations[~parcels_stations.station.isnull()]

        # Join stations to households and jobs
        b['station'] = misc.reindex(parcels_stations.station, b.parcel_id)

        # Filter for those within station buffer
        buildings_stations = b[~b.station.isnull()]

        # Save to main vars
        p = parcels_stations
        b = buildings_stations

    # Get building attributes added up by parcel_id

    b_sum_p = b.fillna(0).groupby('parcel_id').agg('sum')

    # Fill out parcel table with variables of interest

    property_tax = 0.012

    p['buildings'] = b.parcel_id.value_counts()
    p['res_units'] = b_sum_p.residential_units
    p['nonres_sqft'] = b_sum_p.non_residential_sqft

    if baseline:
        p['res_value'] = (b_sum_p.res_price_per_sqft.fillna(0) *
                          b_sum_p.nonres_rent_per_sqft.fillna(0))
        p['nonres_value'] = (b_sum_p.non_residential_price.fillna(0) *
                             b_sum_p.non_residential_sqft.fillna(0))

    else:
        p['res_value'] = (b_sum_p.residential_price.fillna(0) *
                          b_sum_p.residential_sqft.fillna(0))
        p['nonres_value'] = (b_sum_p.non_residential_price.fillna(0) *
                             b_sum_p.non_residential_sqft.fillna(0))

    p['total_improvement_value'] = b_sum_p.res_value + b_sum_p.nonres_value
    p['property_value'] = (p.land_value.fillna(0) +
                           p.total_improvement_value.fillna(0))
    p['tax_revenue'] = p.property_value * property_tax
    p['count'] = 1
    p['land_value'] = p.land_value

    # Assign parcel types

    p.loc[pd.isnull(p.buildings), 'parcel_type'] = 'No Buildings'
    p.loc[(p.res_units > 0) & (p.nonres_sqft > 0), 'parcel_type'] = 'Res and Comm'
    p.loc[(p.res_units > 0) & (p.nonres_sqft == 0), 'parcel_type'] = 'Res'
    p.loc[(p.res_units == 0) & (p.nonres_sqft > 0), 'parcel_type'] = 'Comm'
    p.loc[pd.isnull(p.parcel_type), 'parcel_type'] = 'Other'

    # List of variables of interest

    analysis_vars = ['county_id', 'parcel_type', 'count', 'buildings',
                     'res_units', 'nonres_sqft', 'land_value', 'total_improvement_value',
                     'property_value', 'tax_revenue']

    if counties:
        result = p.fillna(0)[analysis_vars].groupby(['county_id', 'parcel_type']).agg('sum')

    else:
        result = p.fillna(0)[analysis_vars].groupby(['parcel_type']).agg('sum')

    # baseline_df = pd.read_csv(baseline, index_col=['parcel_type'])
    # result = baseline_df.merge(output, left_index=True, right_index=True)

    result.to_csv(out)


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--hdf', type=str, help='HDF5 file with households, parcels, buildings, and jobs tables')
    parser.add_argument('--out', type=str, help='Filepath to save csv to')
    parser.add_argument('--alt', type=int, help='Alternative number')
    parser.add_argument('--county', type=int, help='County level output')
    parser.add_argument('--stations', type=str, help='Stations CSV file')
    parser.add_argument('--net', type=str, help='Network HDF5 file')
    args = parser.parse_args()

    alt, counties, station, net = 99, False, None, None
    if args.alt:
        alt = args.alt
    if args.county:
        counties = True
    if args.stations:
        station = args.stations
    if args.net:
        net = args.net

    summary(hdf=args.hdf,
            out=args.out,
            alternative=alt,
            counties=counties,
            station_csv=station,
            net=net)
