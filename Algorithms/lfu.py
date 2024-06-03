import pandas as pd
from Algorithms.supplement import result_to_json
from dataclasses import dataclass, field

@dataclass(order=True)
class Freq:
    frequency: int
    Item:str = field(compare=False)
    Size:float = field(compare=False)
    def __init__(self, Item, size):
        self.frequency = 0
        self.Item = Item
        self.Size = size
    def access(self):
        self.frequency += 1



def cache_simulator(cache_size:int,workload:pd.DataFrame):
    # lfu_queue = PriorityQueue()
    cached = {}
    cache_fill = 0
    cache_misses = 0
    cache_hits = 0
    for ind in range(len(workload)):
        app = workload['Name'][ind]
        size = workload['BlobBytes'][ind]
        if app in cached:
            cache_hits += 1
            cached[app].access()
            cached = {k: v for k, v in sorted(cached.items(), key=lambda item: item[1], reverse=True)}
        else:
            cache_misses += 1
            if size > cache_size:
                # Too big to cache
                continue
            while (cache_fill + size) > cache_size:
                evicted = cached.popitem()
                cache_fill -= evicted[1].Size
            new = Freq(app,size)
            new.access()
            cached[app] = new
            cached = {k: v for k, v in sorted(cached.items(), key=lambda item: item[1], reverse=True)}
            cache_fill += size
    return cache_hits, cache_misses

def main(workload,cache_size):
   
    hit_result, miss_result = cache_simulator(cache_size,workload)
    result = result_to_json(hit_result,miss_result)
    return result

if __name__ == '__main__':
    print('Please run the main.py file to execute the code.')
