import pandas as pd
import requests
import argparse

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--datafile', type=str, help='Name of data file in indicators API')
parser.add_argument('--csvfile', type=str, help='Path of CSV file to save CSV summary file to')
parser.add_argument('--datadict', type=str, help='Path of CSV file to save data dictionary to')
args = parser.parse_args()

datafile = args.datafile
csvfile = args.csvfile
datadict_file = None
if args.datadict:
    datadict_file = args.datadict

base_url = 'http://localhost:5000/{}/base'.format(datafile)
geo = 'blockgroups'

data_dictionary = {}

# Job Density
options = {'area_unit':'miles'}
data = pd.Series(requests.post(base_url + '/density/jobs/' + geo, options).json())
df = pd.DataFrame(data, columns=['job_density'])
data_dictionary['job_density'] = 'Job Density in Jobs per Square Mile'

# Population Density
options = {'area_unit':'miles'}
data = pd.Series(requests.post(base_url + '/density/population/' + geo, options).json())
df['pop_density'] = data
data_dictionary['pop_density'] = 'Population Density in Persons per Square Mile'

# Median Income
options = {'target_var':'income',
          'spatial_agg':'median'}
data = pd.Series(requests.post(base_url + '/descriptive/households/' + geo, options).json())
df['income_median'] = data
data_dictionary['income_median'] = 'Median Income'

# Mean Income
options = {'target_var':'income',
          'spatial_agg':'mean'}
data = pd.Series(requests.post(base_url + '/descriptive/households/' + geo, options).json())
df['income_mean'] = data
data_dictionary['income_mean'] = 'Mean Income'

# Number of Households
data = pd.Series(requests.get(base_url + '/descriptive/households/' + geo).json())
df['households'] = data
data_dictionary['households'] = 'Number of Households'

# Number of Persons
options = {'target_var':'persons',
          'spatial_agg':'sum'}
data = pd.Series(requests.post(base_url + '/descriptive/households/' + geo, options).json())
df['persons'] = data
data_dictionary['persons'] = 'Total Population'

# Number of Jobs
data = pd.Series(requests.post(base_url + '/descriptive/jobs/' + geo).json())
df['jobs'] = data
data_dictionary['Jobs'] = 'Number of Jobs'

# Households Renting
options = {'hownrent':'2'}
data = pd.Series(requests.post(base_url + '/descriptive/households/' + geo, options).json())
df['hh_rent'] = data
data_dictionary['hh_rent'] = 'Number of Households Renting their Homes'

# Households Own
options = {'hownrent':'1'}
data = pd.Series(requests.post(base_url + '/descriptive/households/' + geo, options).json())
df['hh_own'] = data
data_dictionary['hh_rent'] = 'Number of Households that Own their Homes'

# Number of Buildings
data = pd.Series(requests.get(base_url + '/descriptive/buildings/' + geo).json())
df['buildings'] = data
data_dictionary['buildings'] = 'Number of Buildings'

# Number of Residential Units
options = {'target_var':'residential_units',
          'spatial_agg':'sum'}
data = pd.Series(requests.post(base_url + '/descriptive/buildings/' + geo, options).json())
df['res_units'] = data
data_dictionary['res_units'] = 'Number of Residential Units'

# Residential Square Feet

options = {'target_var':'residential_sqft',
          'spatial_agg':'sum'}
data = pd.Series(requests.post(base_url + '/descriptive/buildings/' + geo, options).json())
df['res_sqft'] = data
data_dictionary['res_sqft'] = 'Total Residential Square Feet'

# Non-Residential Square Feet

options = {'target_var':'non_residential_sqft',
          'spatial_agg':'sum'}
data = pd.Series(requests.post(base_url + '/descriptive/buildings/' + geo, options).json())
df['non_res_sqft'] = data
data_dictionary['non_res_sqft'] = 'Total Non-Residential Square Feet'

naics = {11: 'Agriculture, Forestry, Fishing and Hunting',
         21: 'Mining, Quarrying, and Oil and Gas Extraction',
         22: 'Utilities',
         23: 'Construction',
         3133: 'Manufacturing',
         42: 'Wholesale Trade',
         4445: 'Retail Trade',
         4849: 'Transportation and Warehousing',
         51: 'Information',
         52: 'Finance and Insurance',
         53: 'Real Estate and Rental and Leasing',
         54: 'Professional, Scientific, and Technical Services',
         55: 'Management of Companies and Enterprises',
         56: 'Administrative and Support and Waste Management and Remediation Services',
         61: 'Educational Services',
         62: 'Health Care and Social Assistance',
         71: 'Arts, Entertainment, and Recreation',
         72: 'Accommodation and Food Services',
         81: 'Other Services (except Public Administration)',
         92: 'Public Administration'}

# Jobs by Sector
for key, value in naics.items():
    options = {'sector_id': str(key)}

    try:
        data = pd.Series(requests.post(base_url + '/descriptive/jobs/' + geo, options).json())
    except:
        data = 0

    df['jobs_' + str(key)] = data
    data_dictionary['jobs_' + str(key)] = 'Number of Jobs in ' + value

dd = pd.DataFrame(pd.Series(data_dictionary), columns=['Variable Definition'])
dd.index.name = 'Variable'

dd.to_csv(datadict_file)
df.to_csv(csvfile)