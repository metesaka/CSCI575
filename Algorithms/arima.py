import pandas as pd
import statsmodels.api as sm
import random
import tqdm
from Algorithms.supplement import result_to_json
import warnings
warnings.filterwarnings("ignore")
forecast_steps = 2880
params = (2,0,2)

def set_params(new_params):
    global params
    params = new_params


def calculate_Arima_param(ts_data_np):
    series = ts_data_np.transpose()
    best_count = {}
    for i in range(3):
        for j in range(2):
            for k in range(3):
                params = (i,j,k)
                best_count[params] = 0

    rand_try = random.choices(list(range(2,len(series))),k=30)

    for ind in tqdm.tqdm(rand_try):
        best_arima = {}
        for i in range(3):
            for j in range(2):
                for k in range(3):
                    params = (i,j,k)
                    #print(params)
                    mod = sm.tsa.arima.ARIMA(list(series[ind]), order=params)   
                    best_arima[params] = mod.fit().aic
                    del mod
        best_arima = {k: v for k, v in sorted(best_arima.items(), key=lambda item: item[1])}
        best_count[list(best_arima.keys())[0]] += 2
        best_count[list(best_arima.keys())[1]] += 1


    print(best_count)

class App:
    Name = ''
    Size = 0
    def __init__(self,Name:str,Size:int):
        self.Name = Name
        self.Size = Size
        self.history = []
        self.forec = []
    def update(self,access):
        self.history.append(access)
    def forecast(self,steps:int):
        global params
        mod = sm.tsa.arima.ARIMA(self.history, order=params)   
        res = mod.fit()
        self.forec = list(res.predict(start=len(self.history)+1,end=len(self.history)+steps))
        del mod
    def next_weight(self):
        global forecast_steps
        if len(self.history) == 0:
            return 0
        if len(self.history) < forecast_steps:
            return self.history[-1]
        if len(self.forec) == 0:
            self.forecast(forecast_steps)
        return self.forec.pop(0)
    
class Cache:
    cache_size = 0
    cache_fill = 0
    cache_hits = 0
    cache_misses = 0
    cached = {}
    all_apps = []
    def __init__(self,cache_size:int,apps:list):
        self.cache_size = cache_size
        self.all_apps = apps
    def access(self,app:str):
        if app in self.cached:
            self.cache_hits += 1
            # self.cached = {k: v for k, v in sorted(self.cached.items(), key=lambda item: item[1], reverse=True)}
        else:
            self.cache_misses += 1
    def update(self): 
        weights_ordered = {}
        self.cache_fill = 0 
        self.cached = {}
        for app in self.all_apps:
            weights_ordered[app] = app.next_weight()
        weights_ordered = {k: v for k, v in sorted(weights_ordered.items(), key=lambda item: item[1], reverse=True)}
        for app in weights_ordered:
            if app.Size > self.cache_size:
                # Too big to cache
                continue
            if (self.cache_fill + app.Size) <= self.cache_size:
                self.cached[app.Name] = app
                self.cache_fill += app.Size

def extract_app_data(ts_data:pd.DataFrame,app_data:pd.DataFrame):
    cols = list(ts_data.columns)
    apps = []
    for app in cols[2:]:
        app_info = app_data[app_data['AnonAppName'] == app]
        app_info = app_info.to_numpy()
        one_app = App(app,app_info[0][1])
        # print(one_app.Name,one_app.Size)
        apps.append(one_app)
    return apps

def arima_cache_simulator(ts_data:pd.DataFrame,app_data:pd.DataFrame,cache_size:int):
    ts_data_np = ts_data.to_numpy()
    apps = extract_app_data(ts_data,app_data)
    cache = Cache(cache_size,apps)
    total = len(ts_data_np)
    curr = 0
    for row in ts_data_np:
        curr+=1
        print(f'timespan: {curr}/{total}',end='\r')
        for i in range(2,len(row)):
            apps[i-2].update(row[i])
            if row[i] == 1:
                cache.access(apps[i-2].Name)
        cache.update()
    return result_to_json(cache.cache_hits, cache.cache_misses)

if __name__ == '__main__':
    print('Please run the main.py file to execute the code.')