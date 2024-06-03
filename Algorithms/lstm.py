import pandas as pd
import torch 
import torch.nn as nn
from Algorithms.supplement import result_to_json
from torch.utils.data import Dataset , DataLoader
import warnings
import copy
import numpy as np

warnings.filterwarnings("ignore")
device = 'cuda:0' if torch.cuda.is_available() else 'cpu'
batch_size = 16
lookback = 7
learning_rate = 0.001
num_epochs = 10
loss_function = nn.MSELoss()
counter = 1
train_at = 1440 # 24 hours
params = [batch_size,lookback,learning_rate,num_epochs,train_at]
def train_one_epoch(model,train_loader,optimizer,epoch):
    model.train(True)
    #print(f'Epoch: {epoch + 1}')
    running_loss = 0.0
    
    for batch_index, batch in enumerate(train_loader):
        x_batch, y_batch = batch[0].to(device), batch[1].to(device)
        output = model(x_batch.float())
        loss = loss_function(output.float(), y_batch.float())
        running_loss += loss.item()
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

class TimeSeriesDataset(Dataset):
    def __init__(self, X, y):
        self.X = X
        self.y = y
    def __len__(self):
        return len(self.X)
    def __getitem__(self, i):
        return self.X[i], self.y[i]
    
class LSTM(nn.Module):
    def __init__(self, input_size, hidden_size, num_stacked_layers):
        super().__init__()
        self.hidden_size = hidden_size
        self.num_stacked_layers = num_stacked_layers
        self.lstm = nn.LSTM(input_size, hidden_size, num_stacked_layers, 
                            batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)
    def forward(self, x):
        batch_size = x.size(0)
        h0 = torch.zeros(self.num_stacked_layers, batch_size, self.hidden_size).to(device)
        c0 = torch.zeros(self.num_stacked_layers, batch_size, self.hidden_size).to(device)
        
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return out
    
class App:
    def __init__(self,Name:str,Size:int):
        self.Name = Name
        self.Size = Size
        self.history = []
        self.forec = []
        self.lookback_history = []
        self.ever_trained = False
    def update(self,access):
        self.lookback_history.append(access)
        lookback = params[1]
        if len(self.lookback_history) > lookback:
            lookback_history_copy = copy.deepcopy(self.lookback_history)
            self.history.append(lookback_history_copy)
            self.lookback_history.pop(0)
    def generate_x_y(self):
        history_np = copy.deepcopy(self.history)
        history_np = np.array(history_np)
        #print(history_np.shape)
        X_val = history_np[:, :-1]
        y_val = history_np[:, -1]
        lookback = params[1]
        X_train = X_val.reshape((-1, lookback, 1))
        y_train = y_val.reshape((-1, 1))
        self.X_train = torch.tensor(X_train).float()
        self.y_train = torch.tensor(y_train).float()
        self.train_dataset = TimeSeriesDataset(X_train, y_train)
        batch_size = params[0]
        self.train_loader = DataLoader(self.train_dataset, batch_size=batch_size, shuffle=True)
    def create_model(self):
        global device
        self.model = LSTM(1, 4, 1)
        self.model.to(device)
    def train(self):
        num_epochs = params[3]
        learning_rate = params[2]
        optimizer = torch.optim.Adam(self.model.parameters(), lr=learning_rate)
        for epoch in range(num_epochs):
            train_one_epoch(self.model,self.train_loader,optimizer,epoch)
    def forecast(self):
        global counter
        train_at = params[4]
        lookback = params[1]
        if counter % train_at == 0:
            self.generate_x_y()
            self.create_model()
            self.train()
            counter +=1
            self.ever_trained = True
        if self.ever_trained:
            self.model.train(False)
            self.model.eval()
            lookback_history_copy = copy.deepcopy(self.lookback_history)
            X_n = np.array(lookback_history_copy)
            X_new = X_n.reshape((-1, lookback, 1))
            X_new = torch.tensor(X_new).float()
            with torch.no_grad():
                predicted = self.model(X_new.to(device)).to('cpu').numpy()
            return predicted[0][0]
        else:
            return self.lookback_history[-1]
    
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
            weights_ordered[app] = app.forecast()
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
        apps.append(one_app)
    return apps

def lstm_cache_simulator(ts_data:pd.DataFrame,app_data:pd.DataFrame,cache_size:int):
    ts_data_np = ts_data.to_numpy()
    apps = extract_app_data(ts_data,app_data)
    cache = Cache(cache_size,apps)
    total = len(ts_data_np)
    global counter
    for row in ts_data_np:
        print(f'timespan: {counter}/{total}',end='\r')
        for i in range(2,len(row)):
            apps[i-2].update(row[i])
            if row[i] == 1:
                cache.access(apps[i-2].Name)
        cache.update()
        counter += 1
    return result_to_json(cache.cache_hits, cache.cache_misses)

def set_params(args):
    global params
    params = args


if __name__ == '__main__':
    print('Please run the main.py file to execute the code.')
