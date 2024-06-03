import pandas as pd
from collections import OrderedDict
from Algorithms.supplement import result_to_json

def cache_simulator(cache_size:int,workload:pd.DataFrame):
    cached = OrderedDict()
    cache_fill = 0
    cache_misses = 0
    cache_hits = 0

    for ind in range(len(workload)):
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
            while (cache_fill + size) > cache_size:
                evicted = cached.popitem(last=False)
                cache_fill -= evicted[1]
            cached[app] = size
            cache_fill += size
    return cache_hits, cache_misses

def main(workload,cache_size):
    print('Running LRU')
    hit_result, miss_result = cache_simulator(cache_size,workload)
    result = result_to_json(hit_result,miss_result)
    return result

if __name__ == '__main__':
    print('Please run the main.py file to execute the code.')

