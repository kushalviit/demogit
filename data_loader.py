import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time

class DataGen:
    def __init__(self, data_file_path, batch_size=32):
        self.data_file_path = data_file_path
        self.data = self.load_data()
        self.length = len(self.data)
        self.bath_size = batch_size
        self.iter_count = 0
        self.iter = self.set_iter()

    def set_iter(self):
        if self.length % self.bath_size == 0:
            return self.length // self.bath_size
        else:
            return self.length // self.bath_size + 1
    
    def get_iter(self):
        return self.iter
    
    def load_data(self):
        df = pd.read_csv(self.data_file_path)
        df = pd.read_csv(filename)
        df = df.dropna()
        gene_names = df.columns.to_list()[1:]
        return df[gene_names].to_numpy()

    
    def get_batch(self,begin_idx,end_idx,i,e,lock):
        with lock:
            print(f'epoch: {e},iteration: {i}')
            return self.data[begin_idx:end_idx,:]
    
    def data_gen(self,epoch):
        for j in range(self.iter):
            print(f'epoch:{epoch},iteration: {j}')
            yield self.data[j*self.bath_size:(j+1)*self.bath_size,:]
        
    
    def display(self):
        print(self.data[0:10,:])
        print(self.data.shape)
        print(self.length)
        print(self.bath_size)

#### hi this new comment




filename = '../drugseek/data/processed_OmicsExpression.csv'


batch_size =6
batch_processor = DataGen(filename, batch_size=batch_size)


#batch_processor.display()
i =0
iter = batch_processor.get_iter()
print(f'Iter Count: {iter}')


parallel = False

max_workers = 10
time_exec =0

while(i<100):
    print(f'Epoch: {i}')
    start_time = time.time()
    if parallel:
        lock = Lock()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for k in range(iter):
                print(executor.submit(batch_processor.get_batch, k*batch_size, (k+1)*batch_size,k,i,lock).result().shape)
        time_exec = time_exec + (time.time()-start_time)
        
    else:
        for batch in batch_processor.data_gen(epoch=i):
            print(f'batch size: {batch.shape}')
        time_exec =  time_exec + (time.time()-start_time)
    print(f'Avg Time: {time_exec/100}')
    
    i+=1

