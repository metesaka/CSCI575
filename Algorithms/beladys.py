import pandas as pd
from collections import OrderedDict
from Algorithms.supplement import result_to_json
import tqdm

def find_highest_future_access(current_index, current_cache, workload):
    future_order = OrderedDict()
    app_names = workload['Name'].tolist()
    # print(len(app_names))
    # print(current_index)
    for i in range(current_index+1,len(app_names)):
        app = app_names[i]   
        if (app in current_cache) and (not(app in future_order)):
                future_order[app] = True
    # print(future_order)
    return future_order

def cache_simulator(cache_size:int,workload:pd.DataFrame):
    cached = OrderedDict()
    cache_fill = 0
    cache_misses = 0
    cache_hits = 0

    for ind in tqdm.trange(len(workload)):
        app = workload['Name'][ind]
        size = workload['BlobBytes'][ind]
        if app in cached:
            cache_hits += 1
            cached.move_to_end(app)
        else:
            cache_misses += 1
            if size > cache_size:
                # Too big to cache
                continue
            if (cache_fill + size) > cache_size:
                # print('eviction needed')
                future = find_highest_future_access(ind,cached,workload)
                # print('eviction order found')
            while (cache_fill + size) > cache_size:
                if len(future) == 0:
                    # print('lru eviction needed')
                    evicted = cached.popitem(last=False)
                    cache_fill -= evicted[1]
                else:    
                    evicted = future.popitem(last=True)
                    cache_fill -= cached[evicted[0]]
                    del cached[evicted[0]]            
            cached[app] = size
            cache_fill += size
    return cache_hits, cache_misses

def main(workload,cache_size):
    print("Running Belady's Algorithm")
    hit_result, miss_result = cache_simulator(cache_size,workload)
    result = result_to_json(hit_result,miss_result)
    return result

if __name__ == '__main__':
    print('Please run the main.py file to execute the code.')

