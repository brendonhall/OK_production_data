import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
from mpl_toolkits.basemap import Basemap

from pandas import set_option

#not sure if you have seaborn installed...
#import seaborn as sb


plt.close('all')
set_option("display.max_rows", 12)

#load the OK production data into a pandas dataframe.
#each row contains a single monthly reported production total
#for a single well 
all_production = pd.read_hdf('All_OK_production_data.h5', 'OK_production')


##find the total production for each well
#production_totals = all_production.groupby(['API_NUMBER']).sum()
##sort to find the top gas producersall
#production_totals.sort_values(by=['GAS'], ascending=0, inplace=True)
#largest_gas_well_ids = production_totals.iloc[0:100].index.values
#
##plot the production curves for a number of top producing wells
#for i in np.arange(0,10):
#    production = all_production[all_production['API_NUMBER'] == largest_gas_well_ids[i]] 
#    production.set_index('PROD_DATE', inplace=True)
#    production['GAS'].interpolate(method='time',inplace=True)
#    production['Natural Log'] = production['GAS'].apply(lambda x: np.log(x))
#    fig, ax = plt.subplots(2, 1) 
#    production['GAS'].plot(ax=ax[0], title='Production vs. Time for well %s'%largest_gas_well_ids[i])
#    production['Natural Log'].plot(ax=ax[1], title='Log Production vs. Time')
#    fig.tight_layout()  
#    plt.show()
