#this file takes the cleaned Oklahoma production data and
#converts it into a dataframe that has each montly total for each
#well in its own row.  This is to make it easier to select all the
#data for a given well ID

import pandas as pd
import numpy as np

import time

#data = pd.read_hdf('data/OK_all_cleaned_data.h5', 'OK_production')
data = pd.read_csv('~/Desktop/DATA/Projects/OK_production_data/OK_prod_data.csv')
data.reset_index(inplace=True)
months = ['January', 'February', 'March', 'April', 'May', 'June',
          'July', 'August', 'September', 'October', 'November']

all_production = pd.DataFrame(columns=('API_NUMBER', 'PROD_DATE', 'GAS', 'OIL'))

datafile = pd.HDFStore('ALL_OK_production_data.h5', mode='w')

start = time.time()
for index, row in data.iterrows():
    #create an empty dataframe to hold the production from this
    #well for the year
    production = pd.DataFrame(columns=('API_NUMBER', 'PROD_DATE', 'GAS', 'OIL'))
    
    #all of the production for a single year is in each row.
    #create datetime strings for each reporting period
    year = str(int(row['Year']))
    timestamps = []
    for month_num in np.arange(1,13,1):
        timestamps.append(year + '-' + '%02d'%month_num)

    production['PROD_DATE'] = pd.to_datetime(pd.Series(timestamps))
    production.loc[:,'API_NUMBER'] = row['API_NUMBER'] #
    #row.iloc[4:27:2].fillna(-1,inplace=True)
    
    #get the production by directly indexing the columns
    #(not the prettiest, but compact)
    production['GAS'] = row.iloc[1:25:2].values
    production.loc[:,'OIL'] = row.iloc[2:26:2].values
    #production['GAS'] = row.iloc[5:29:2].values
    #production.loc[:,'OIL'] = row.iloc[4:27:2].values
    
    #convert the production totals to numeric types
    production['GAS'] = production['GAS'].astype(float)
    production['OIL'] = production['OIL'].astype(float)    

    #add the production of this well to the running total
    all_production = pd.concat([all_production, production])
    
    #every 10000 entries will be saved to the HDF archive on
    #disk
    if (index % 10000) == 0:
        print index
        datafile.append('OK_production', all_production, min_itemsize=30)
        #empty all_production
        all_production = pd.DataFrame(columns=('API_NUMBER', 'PROD_DATE', 'GAS', 'OIL'))

    #this takes a while to run on the whole dataset, so 
    #we may want to stop early when debugging...
    #if (index == 10):
    #    break
        
end = time.time()

print 'Production extraction took %d seconds.'%(end - start)
datafile.close()
