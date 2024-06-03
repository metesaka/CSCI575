import pandas as pd
from Algorithms.supplement import find_app_data_size,generate_app_based_workload,generate_blob_based_workload
from Algorithms import optimal
from Algorithms import lru
from Algorithms import fifo
from Algorithms import beladys
from Algorithms import rand
from Algorithms import prophetTS
from Algorithms import lstm
from Algorithms import arima
from Algorithms import convert_to_ts
import os
os.environ['KMP_DUPLICATE_LIB_OK']='True'
def load_workloads(RW):
    if input('Do you want to use the sample data? (y/n)') == 'y':
        azure_data_path = './data/sample.csv'
        app_data_size_path = f'./data/app_data_size_{RW}.csv'
        app_workload_path = f'./data/app_workload_{RW}.csv'
        blob_workload_path = f'./data/blob_workload_{RW}.csv'
        ts_data_path = f'./data/All_App_access_{RW}.csv'
    else:
        print('Please place the data in the data folder')
        data_name = input('Enter the name of the data file: ')
        azure_data_path = f'./data/{data_name}.csv'
    try:
        app_data_size = pd.read_csv(app_data_size_path)
        print('app_data_size loaded')
    except:
        print('app_data_size not found')    
        app_data_size = find_app_data_size(azure_data_path,RW,app_data_size_path)
    try:
        appWorkload = pd.read_csv(app_workload_path)
        print('app_workload loaded')
    except:
        print('app_workload not found')
        appWorkload = generate_app_based_workload(azure_data_path,RW,app_workload_path)
    try:
        blobWorkload = pd.read_csv(blob_workload_path)
        print('blob_workload loaded')
    except:
        print('blob_workload not found')
        blobWorkload = generate_blob_based_workload(azure_data_path,RW,blob_workload_path)
    try:
        ts_data = pd.read_csv(ts_data_path)
        print('ts_data loaded')
    except:
        convert_to_ts.main(azure_data_path)
        ts_data = pd.read_csv(ts_data_path)
    appWorkload = appWorkload.merge(app_data_size, on='AnonAppName')
    appWorkload.columns = ['Timestamp', 'Name', 'BlobBytes']
    blobWorkload.columns = ['Timestamp', 'Name', 'BlobBytes']
    appWorkload = appWorkload.sort_values(by='Timestamp')
    blobWorkload = blobWorkload.sort_values(by='Timestamp')
    return appWorkload, blobWorkload, ts_data, app_data_size

def get_lstm_params():
    batch_size = 16
    lookback = 7
    learning_rate = 0.001
    num_epochs = 10
    counter = 1
    train_at = 1440 # 24 hours
    params = [batch_size,lookback,learning_rate,num_epochs,train_at]
    print('Current parameters:')
    print(f'Batch size: {batch_size}, Lookback: {lookback}, Learning rate: {learning_rate}, Number of epochs: {num_epochs}, Train at: {train_at}')    
    new_params = input('Do you want to change the default parameters for the LSTM model? (y/n)')
    if new_params == 'y':
        batch_size = int(input('Enter the batch size: '))
        lookback = int(input('Enter the lookback: '))
        learning_rate = float(input('Enter the learning rate: '))
        num_epochs = int(input('Enter the number of epochs: '))
        train_at = int(input('Enter the time to start training: '))
        params = [batch_size,lookback,learning_rate,num_epochs,train_at]
    return params


def main(args):
    model,appWorkload, blobWorkload,app_data_size, ts_workload, readOrWrite,cache_size,dataset = args
    datasets = {'app': appWorkload, 'blob': blobWorkload}
   
    #for dataset in datasets:
    print(f'Running {model} on {dataset} with cache size {cache_size}')
    if model == 'optimal':
        result = optimal.main(datasets[dataset], cache_size)
    elif model == 'lru':
        result = lru.main(datasets[dataset], cache_size)
    elif model == 'fifo':
        result = fifo.main(datasets[dataset], cache_size)
    elif model == 'beladys':
        result = beladys.main(datasets[dataset], cache_size)
    elif model == 'random':
        result = rand.main(datasets[dataset], cache_size)
    elif model == 'prophetTS':
        result = prophetTS.prophet_cache_simulator(ts_workload,app_data_size,cache_size)
    elif model == 'lstm':
        lstm.set_params(get_lstm_params())
        result = lstm.lstm_cache_simulator(ts_workload,app_data_size,cache_size)
    elif model == 'arima':
        new_param = input('Do you want to change the default parameters (2,0,2) for the ARIMA model? (y/n)')
        if new_param == 'y':
            p = int(input('Enter the p value: '))
            d = int(input('Enter the d value: '))
            q = int(input('Enter the q value: '))
            arima.set_params(p,d,q)
        result = arima.arima_cache_simulator(ts_workload,app_data_size,cache_size)
    with open(f'caching_results.txt', 'a') as f:
        f.write(f'{model}-{cache_size}-{dataset}\n')
        f.write(str(result)+'\n')
    print(f'{model}-{cache_size}-{dataset} done')



if __name__ == '__main__':
    readOrWrite = 'Read'
    appWorkload, blobWorkload, ts_workload, app_data_size = load_workloads(readOrWrite)
    models = ['optimal','lru','fifo','beladys','random','prophetTS','lstm','arima']
    model = input('Enter the model to run (type all to test them all): ')
    if model != 'all':
        while model not in models:
            print('Invalid model')
            print('Models available: "optimal", "lru", "fifo", "beladys", "rand", "prophetTS", "lstm", "arima"')
            model = input('Enter the model to run: ')
    cache_size = int(input('Enter the cache size in bytes: '))
    if model in ['prophetTS','lstm','arima']:
        dataset = 'app'
    else:
        dataset = input('Enter the dataset to run on (app/blob): ')
    while dataset not in ['app','blob']:
        print('Invalid dataset')
        dataset = input('Enter the dataset to run on (app/blob): ')
    
    if model == 'all':
        for model in models:
            main((model,appWorkload, blobWorkload,app_data_size, ts_workload, readOrWrite,cache_size,dataset))
    else:
        main((model,appWorkload, blobWorkload,app_data_size, ts_workload, readOrWrite,cache_size,dataset))
