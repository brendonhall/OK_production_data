import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os.path

from pandas import set_option
set_option("display.max_rows", 12)

plt.close('all')

data_dir = '~/Desktop/DATA/Projects/OK_production_data/historical/'

# generate a list of years that will be used to form filenames
years = np.arange(1988, 2016, 1)

# The data for 1994 is missing, so drop it from the list
years = np.delete(years, np.where(years==1994))

#initialize list that will hold annual production data frames
data_frames = []
location_data_list = []

for year in years:
    
    filename = data_dir + str(year)+'prodn.txt'
    if not os.path.isfile(filename):
        filename = data_dir + str(year)+'PRODN.TXT'
    print filename
    
    if year <= 2008:
        
        # there are 6 lines in years between 1990 and 1995 that have
        # an extra field.  This could be due to an extra delimiter
        # '|' in the line.  These are ignored.
        data = pd.read_csv(filename, sep="|", engine='c', 
                        doublequote=False,error_bad_lines=False,
                        low_memory=False)
        
        #remove the first row with ------
        data = data.ix[1:]
        
        #remove the whitespace from the column names
        data = data.rename(columns=lambda x: x.strip())
            
        # last GAS column has whitespace that will affect the 
        #name assigned to it. Rename it manually.        
        data.columns.values[-1] = 'GAS.11'
        
    # data after 2008 is formatted in a more consistent way
    # that requires less work to clean up.  However, the 2012 
    # data file begins with an extra line we need to ignore.    
    else:
        nskip = 0
        if (year == 2012):
            nskip = 1
        data = pd.read_csv(filename, sep="|", engine='c', 
                        doublequote=False,error_bad_lines=False,
                        skiprows=nskip)
        
    # there are quite a few columns we don't need.  
    #column_idx = [1,15,23,16,31,32,34,35,37,38,40,41,43,44,46,
    #              47,49,50,52,53,55,56,58,59,61,62,64,65,66]
    #data = data[column_idx]
    
    data.dropna(subset=['API_NUMBER'], inplace=True)
    
    data.drop(['WELL_NAME', 'OPER_NO',
               'OPERATOR', 'ME', 'SECTION',
               'TWP', 'RAN', 'Q4', 'Q3', 'Q2', 'Q1', 'OFB',
               'ALLOWABLE_CLASS', 'ALLOWABLE_TYPE', 'PURCH_NO',
               'PURCHASER', 'YEAR', 'JAN', 'FEB', 'MAR', 'APR',
               'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC',
               'CODE', 'OTC_COUNTY','OTC_LEASE_NO', 'OTC_MERGE',
               'OTC_SUB_NO', 'POOL_NO', 'PURCH_SUFFIX','SUFFIX', 
               'OPER_SUFFIX', 'OFB.1','S'],
               axis=1, inplace=True, errors='ignore')
    
    data['WELL_NO'] = data['WELL_NO'].str.strip()
    data['API_NUMBER'] = data['API_NUMBER'].astype(int, 
        raise_on_error=False).apply(lambda x: '{0:0>5}'.format(x))
    data['API_COUNTY'] = data['API_COUNTY'].astype(int,
        raise_on_error=True).apply(lambda x: '{0:0>3}'.format(x))
    # Make API number in standard form
    # see https://en.wikipedia.org/wiki/API_well_number#cite_note-SPWLA-4
    data['API_NUMBER'] = '35'+data['API_COUNTY']+data['API_NUMBER']+'0000'
    
    data.drop(['WELL_NO', 'API_COUNTY'], axis=1, inplace=True)
    
    for col_name in list(data.columns.values):
        if ('GAS' in col_name) or ('OIL' in col_name):
            data[col_name] = pd.to_numeric(data[col_name], errors='coerce')
    
    data['LATITUDE'] = pd.to_numeric(data['LATITUDE'],errors='coerce')
    
    location_data = data[['API_NUMBER', 'LATITUDE', 'LONGITUDE', 
                      'FORMATION',]]
    data.drop(['LATITUDE', 'LONGITUDE', 
                    'FORMATION'], axis=1, inplace=True)
    
    #some entity id's have multiple entries.  I do not know 
    #why this is.  it appears like they might be the production
    #attributed to different owners. try summing the production
    #for now...
    data = data.groupby(by='API_NUMBER').sum()
    #data.reset_index()
    
    data.loc[:,'Year'] = year
    
    data_frames.append(data)
    location_data_list.append(location_data)
    
all_data = pd.concat(data_frames)
all_location_data = pd.concat(location_data_list)

all_data.rename(columns={
    'GAS': 'GAS - January', 'OIL': 'OIL - January',
    'GAS.1': 'GAS - February', 'OIL.1': 'OIL - February',
    'GAS.2': 'GAS - March', 'OIL.2': 'OIL - March',
    'GAS.3': 'GAS - April ','OIL.3': 'OIL - April',
    'GAS.4': 'GAS - May', 'OIL.4': 'OIL - May',
    'GAS.5': 'GAS - June','OIL.5': 'OIL - June',
    'GAS.6': 'GAS - July','OIL.6': 'OIL - July',
    'GAS.7': 'GAS - August','OIL.7': 'OIL - August',
    'GAS.8': 'GAS - September','OIL.8': 'OIL - September',
    'GAS.9': 'GAS - October','OIL.9': 'OIL - October',
    'GAS.10': 'GAS - November','OIL.10': 'OIL - November',
    'GAS.11': 'GAS - December','OIL.11': 'OIL - December'}, 
    inplace=True)
                    
#now clean the formation data
def clean_formation_string(in_string):
    
    out_string = str(in_string).strip()
    return " ".join(out_string.split())
    
all_location_data["FORMATION"] = all_location_data["FORMATION"].astype(str) 
all_location_data["FORMATION"] = all_location_data["FORMATION"].apply(clean_formation_string) 

all_location_data['LATITUDE'] = pd.to_numeric(all_location_data['LATITUDE'],
                                     errors='coerce')
all_location_data['LATITUDE'] = np.where(np.abs(all_location_data['LATITUDE']) > 40, 
                                np.nan, all_location_data['LATITUDE'])
all_location_data['LATITUDE'] = np.where(np.abs(all_location_data['LATITUDE']) < 30, 
                                np.nan, all_location_data['LATITUDE'])
fig,ax = plt.subplots()
all_location_data['LATITUDE'].hist(bins=40)
ax.set_yscale('log')
plt.show()

all_location_data['LONGITUDE'] = pd.to_numeric(all_location_data['LONGITUDE'],
                                      errors='coerce')
all_location_data['LONGITUDE'] = np.where(all_location_data['LONGITUDE'] < -110, 
                                np.nan, all_location_data['LONGITUDE'])
all_location_data['LONGITUDE'] = np.where(all_location_data['LONGITUDE'] > -90, 
                                np.nan, all_location_data['LONGITUDE'])
all_location_data['LONGITUDE'] = np.abs(all_location_data['LONGITUDE']) * -1
fig,ax = plt.subplots()
all_location_data['LONGITUDE'].hist(bins=50)
ax.set_yscale('log')
plt.show()

#all_data.to_hdf('OK_cleaned_data.h5', 'OK_production', 
#                 mode='w', complib='zlib')
#
all_data.to_csv(data_dir +'OK_prod_data.csv')

all_location_data.to_csv(data_dir +'OK_location_data.csv', index=False)