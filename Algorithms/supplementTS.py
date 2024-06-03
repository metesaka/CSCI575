import pandas as pd
def find_time_interval(data_path:str, time_col_name:str ) -> list:
    data = pd.read_csv(data_path,usecols=[time_col_name])
    time_endpoints = [data[time_col_name].min(),data[time_col_name].max()]
    return time_endpoints

def get_time_series_template(interval:list, period:str):
    if period == 'minute':
        unit = 'm'
        freq = 'T'
    elif period == 'second':
        unit = 's'
        freq = 'S'
    elif period == 'millisecond':
        unit = 'ms'
        freq = 'L'
    full_timespan = pd.DataFrame(pd.date_range(start=pd.to_datetime(interval[0],unit=unit),end=pd.to_datetime(interval[1],unit=unit),freq=freq),columns=['DateTime'])
    full_timespan['Timestamp'] = [i for i in range(interval[0],interval[1]+1)]
    return full_timespan


if __name__ == '__main__':
    print('Please run the main.py file to execute the code.')
