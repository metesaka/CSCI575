import pandas as pd
import Algorithms.supplementTS as supplementTS
import tqdm



def generate_cumsum(All_APP_TS:pd.DataFrame,apps:list,data:pd.DataFrame,full_timespan:pd.DataFrame):
    for app in tqdm.tqdm(apps):
        one_app = data[data['AnonAppName'] == app].sort_values(by='Timestamp').drop(columns=['AnonAppName'])
        one_app['IsAccessed'] = 1
        one_app_TS = one_app.merge(full_timespan,on='Timestamp',how='right')
        one_app_TS = one_app_TS.fillna(0)
        one_app_TS['Weight'] = one_app_TS['IsAccessed'].cumsum()
        All_APP_TS[app] = one_app_TS['Weight']
    All_APP_TS.to_csv('../workloads/All_APP_TS_Read.csv',index=False)

def generate_access(All_APP_TS:pd.DataFrame,apps:list,data:pd.DataFrame,full_timespan:pd.DataFrame):
    for app in tqdm.tqdm(apps):
        one_app = data[data['AnonAppName'] == app].sort_values(by='Timestamp').drop(columns=['AnonAppName'])
        one_app['IsAccessed'] = 1
        one_app_TS = one_app.merge(full_timespan,on='Timestamp',how='right')
        one_app_TS = one_app_TS.fillna(0)
        #one_app_TS['Weight'] = one_app_TS['IsAccessed'].cumsum()
        All_APP_TS[app] = one_app_TS['IsAccessed']
    All_APP_TS.to_csv('./data/All_App_access_Read.csv',index=False)

def main(data_path):

    time_interval = supplementTS.find_time_interval(data_path,'Timestamp')
    time_interval_minutes = [time//60000 for time in time_interval]
    print('Data Loading...')
    data = pd.read_csv(data_path,usecols=['Timestamp','AnonAppName','Read'])
    data['Timestamp'] = data['Timestamp']//60000 # convert to minutes
    data = data[data['Read'] == True].drop(columns=['Read'])
    apps = data['AnonAppName'].unique()
    full_timespan = supplementTS.get_time_series_template(time_interval_minutes,'minute')

    All_APP_TS = full_timespan.copy()
    generate_access(All_APP_TS,apps,data,full_timespan)
    # generate_cumsum(All_APP_TS,apps,data,full_timespan)

if __name__ == '__main__':
    pass