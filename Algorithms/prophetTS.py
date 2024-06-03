import pandas as pd
from prophet import Prophet
from Algorithms.supplement import result_to_json
import warnings
warnings.filterwarnings("ignore")
forecast_steps = 2880

class App:
    Name = ''
    Size = 0
    def __init__(self,Name:str,Size:int):
        self.Name = Name
        self.Size = Size
        self.history = pd.DataFrame(columns=['ds','y'])
        self.forec = []
    def update(self,access):
        access = pd.DataFrame({'ds':[access[0]],'y':[access[1]]})
        self.history = pd.concat([self.history,access])
    def forecast(self,steps:int):
        global forecast_steps
        m = Prophet()        
        m.fit(self.history)
        future = m.make_future_dataframe(periods=forecast_steps, freq='t')
        forecast = m.predict(future)
        forecasts = list(forecast['yhat'])
        self.forec = forecasts[-forecast_steps:]
        del m, future, forecast
    def next_weight(self):
        global forecast_steps
        if len(self.history) == 0:
            return 0
        if len(self.history) < forecast_steps:
            return self.history.iloc[-1,1]
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

def prophet_cache_simulator(ts_data:pd.DataFrame,app_data:pd.DataFrame,cache_size:int):
    ts_data_np = ts_data.to_numpy()
    apps = extract_app_data(ts_data,app_data)
    cache = Cache(cache_size,apps)
    total = len(ts_data_np)
    curr = 0
    for row in ts_data_np:
        curr+=1
        print(f'timespan: {curr}/{total}',end='\r')
        for i in range(2,len(row)):
            apps[i-2].update([row[0],row[i]])
            if row[i] == 1:
                cache.access(apps[i-2].Name)
        cache.update()
    return result_to_json(cache.cache_hits, cache.cache_misses)

if __name__ == '__main__':
    print('Please run the main.py file to execute the code.')