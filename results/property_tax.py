from __future__ import division
import pandas as pd
import argparse
from pandana.network import Network
from urbansim.utils import misc

# Initial options
pd.set_option('display.float_format', lambda x: '%.3f' % x)

def region(baseline, hdf, out):

    with pd.HDFStore(hdf) as store:
        p = store.parcels
        b = store.buildings

    # Get building attributes added up by parcel_id

    buildings_sum_parcels = b.fillna(0).groupby('parcel_id').agg('sum')

    # Fill out parcel table with variables of interest

    property_tax = 0.012

    p['buildings_2035'] = b.parcel_id.value_counts()
    p['res_units_2035'] = buildings_sum_parcels.residential_units
    p['nonres_sqft_2035'] = buildings_sum_parcels.non_residential_sqft
    p['total_improvement_value_2035'] = buildings_sum_parcels.improvement_value
    p['property_value_2035'] = p.land_value.fillna(0) + p.total_improvement_value.fillna(0)
    p['tax_revenue_2035'] = p.property_value * property_tax
    p['count_2035'] = 1
    p['land_value_2035'] = p.land_value

    # Assign parcel types

    p.loc[pd.isnull(p.buildings), 'parcel_type'] = 'No Buildings'
    p.loc[(p.res_units > 0) & (p.nonres_sqft > 0), 'parcel_type'] = 'Res and Comm'
    p.loc[(p.res_units > 0) & (p.nonres_sqft == 0), 'parcel_type'] = 'Res'
    p.loc[(p.res_units == 0) & (p.nonres_sqft > 0), 'parcel_type'] = 'Comm'
    p.loc[pd.isnull(p.parcel_type), 'parcel_type'] = 'Other'

    # List of variables of interest

    analysis_vars = ['county_id', 'parcel_type', 'count_2035', 'buildings_2035',
                     'res_units_2035', 'nonres_sqft_2035', 'land_value_2035', 'total_improvement_value_2035',
                     'property_value_2035', 'tax_revenue_2035']

    output = p.fillna(0)[analysis_vars].groupby(['parcel_type']).agg('sum')
    # counties = p.fillna(0)[analysis_vars].groupby(['county_id', 'parcel_type']).agg('sum')

    baseline_df = pd.read_csv(baseline, index_col=['parcel_type'])

    result = baseline_df.merge(output, left_index=True, right_index=True)

    result.to_csv(out)


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--base', type=str, help='Path to baseline csv file')
    parser.add_argument('--hdf', type=str, help='HDF5 file with households, parcels, buildings, and jobs tables')
    parser.add_argument('--out', type=str, help='Filepath to save csv to')
    args = parser.parse_args()

    region(baseline=args.base,
           hdf=args.hdf,
           out=args.out)
