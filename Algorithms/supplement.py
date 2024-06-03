import pandas as pd

def generate_blob_based_workload(data:str,RW='Read',output_path=None)->pd.DataFrame:
    ''''
    This function takes in the Azure data and filter the data by the type of memort access to be considered.
    Then it sorts the data by the timestamp. Outputs the data to a csv file (Timestamp, BlobName)
    :param data: The path to the Azure data
    :param RW: The type of data to be considered. It can be 'Read', 'Write' or 'Both'
    :param output_path: The path to save the workload data
    '''
    if not output_path:
        output_path = f'./blob_workload_{RW}.csv'
    data = pd.read_csv(data,usecols=['Timestamp','AnonBlobName','BlobBytes','Read'])
    #data = pd.read_csv(data,usecols=['Timestamp','AnonBlobName','BlobBytes','Read'],nrows=100000)
    if RW == 'Read':
        only_read = data[data['Read'] == True]
        blob_times = only_read.drop(columns=['Read']).sort_values(by='Timestamp')
    elif RW == 'Write':
        only_write = data[data['Read'] == False]
        blob_times = only_write.drop(columns=['Read']).sort_values(by='Timestamp')
    elif RW == 'Both':
        blob_times.drop(columns=['Read'],inplace=True)
        blob_times = data.sort_values(by='Timestamp')
    else:
        raise ValueError('The value of RW can only be "Read" or "Write" or "Both"')
    blob_times.to_csv(output_path,index=False)
    return blob_times


def find_app_data_size(data:str,RW='Read',output_path=None)->pd.DataFrame:
    ''''
    This function takes in the Azure data and finds the total data size by each app.
    Then it outputs the data to a csv file (AnonAppName, BlobBytes)
    :param data: The path to the Azure data
    :param RW: The type of data to be considered. It can be 'Read', 'Write' or 'Both'
    '''
    if not output_path:
        output_path = f'./app_data_size_{RW}.csv'
    data = pd.read_csv(data,usecols=['AnonAppName','BlobBytes','Read'])
    # data = pd.read_csv(data,usecols=['AnonAppName','BlobBytes','Read'],nrows=100000)
    if RW == 'Read':
        only_read = data[data['Read'] == True]
        data_by_app = only_read.drop(columns=['Read']).groupby('AnonAppName',as_index=False).sum()
    elif RW == 'Write':
        only_write = data[data['Read'] == False]
        data_by_app = only_write.drop(columns=['Read']).groupby('AnonAppName',as_index=False).sum()
    elif RW == 'Both':
        data_by_app = data.drop(columns=['Read']).groupby('AnonAppName',as_index=False).sum()
    else:
        raise ValueError('The value of RW can only be "Read", "Write" or "Both"')
    data_by_app.to_csv(output_path,index=False)
    return data_by_app

def generate_app_based_workload(data:str,RW='Read',output_path=None)->pd.DataFrame:
    ''''
    This function takes in the Azure data and filter the data by the type of memort access to be considered.
    Then it sorts the data by the timestamp. Outputs the data to a csv file (Timestamp, AnonAppName)
    :param data: The path to the Azure data
    :param RW: The type of data to be considered. It can be 'Read', 'Write' or 'Both'
    :param output_path: The path to save the workload data
    '''
    if not output_path:
        output_path = f'./workload_{RW}.csv'
    data = pd.read_csv(data,usecols=['Timestamp','AnonAppName','Read'])
    # data = pd.read_csv(data,usecols=['Timestamp','AnonAppName','Read'],nrows=100000)
    if RW == 'Read':
        only_read = data[data['Read'] == True]
        app_times = only_read.drop(columns=['Read']).sort_values(by='Timestamp')
    elif RW == 'Write':
        only_write = data[data['Read'] == False]
        app_times = only_write.drop(columns=['Read']).sort_values(by='Timestamp')
    elif RW == 'Both':
        app_times.drop(columns=['Read'],inplace=True)
        app_times = data.sort_values(by='Timestamp')
    else:
        raise ValueError('The value of RW can only be "Read" or "Write" or "Both"')
    app_times['Timestamp'] = app_times['Timestamp']//60000
    app_times.drop_duplicates(inplace=True)
    app_times.to_csv(output_path,index=False)
    return app_times

def result_to_json(hit_result,miss_result):
    ''''
    This function takes in the results of the cache simulation and returns a json object
    :param hit_result: The number of cache hits
    :param miss_result: The number of cache misses
    '''
    result = {
        'cache_hits': hit_result,
        'cache_misses': miss_result,
        'hit_ratio': hit_result/(hit_result+miss_result),
        'miss_ratio': miss_result/(hit_result+miss_result),
        'total_requests': hit_result+miss_result
    }
    return result


