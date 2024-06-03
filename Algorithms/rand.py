import pandas as pd
from Algorithms.supplement import result_to_json
import random


def cache_simulator(cache_size:int,workload:pd.DataFrame):
    ''''
    CACHE WITH FIFO REPLACEMENT POLICY 
    This function takes in the cache size and the workload and simulates the cache.
    Returns the number of cache hits and cache misses
    :param cache_size: The size of the cache in bytes
    :param workload: The workload to be simulated
    '''
    cached = {}
    cache_fill = 0
    cache_misses = 0
    cache_hits = 0

    for ind in range(len(workload)):
        app = workload['Name'][ind]
        size = workload['BlobBytes'][ind]
        if app in cached:
            cache_hits += 1
        else:
            cache_misses += 1
            if size > cache_size:
                # Too big to cache
                continue
            elif (cache_fill + size) > cache_size:
                # eviction
                while (cache_fill + size) > cache_size:
                    evict_key = random.choice(list(cached.keys()))
                    cache_fill -= cached[evict_key]
                    cached.pop(evict_key)
            cache_fill += size
            cached[app] = size

    return cache_hits, cache_misses

def main(workload, cache_size):
    hit_result, miss_result = cache_simulator(cache_size,workload)
    result = result_to_json(hit_result,miss_result)
    return result

if __name__ == '__main__':
    print('Please run the main.py file to execute the code.')


