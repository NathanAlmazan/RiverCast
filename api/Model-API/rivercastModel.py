import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
import calendar
import math
import torch
import torch.nn as nn
import numpy as np
import scipy as sc
from sklearn.preprocessing import MinMaxScaler
from skimage.measure import block_reduce
from sklearn.metrics import mean_absolute_error

#IMPORTING
device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')  # configure GPU utilization
device

df = pd.read_csv('riverlevel.csv')  # import dataset

# convert month name to integer
month_dict = dict((v, k) for k, v in enumerate(calendar.month_name))
df['Month'] = df['Month'].map(month_dict)

# create datetime column
df[['Year', 'Month', 'Day', 'Hour']] = df[['Year', 'Month', 'Day', 'Hour']].astype(int)
df['Hour'] = df['Hour'].apply(lambda x: x if x < 24 else 0)

# convert year, month, day, and hour columns into timestamp
df['Datetime'] = df[['Year', 'Month', 'Day', 'Hour']].apply(lambda row: datetime(row['Year'], row['Month'], row['Day'], row['Hour']).isoformat(), axis=1)
df["Datetime"] = pd.to_datetime(df["Datetime"], format='ISO8601')

# assign timestamps as the data frame index
df.index = df["Datetime"]
df = df.drop(['Datetime'], axis=1)

# select the parameters
df = df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3', 'RF-Intensity', 'RF-Intensity.1', 'RF-Intensity.2', 'RF-Intensity.3', 'Precipitation', 'Precipitation.1', 'Precipitation.2', 'Humidity', 'Humidity.1', 'Humidity.2', 'Temperature', 'Temperature.1', 'Temperature.2']] 
df = df.astype(np.float64)  # convert parameters into a double precision floating number

# fill in missing values using linear interpolation
df = df.interpolate(method='linear', limit_direction='forward')
df = df.resample('6H').max()  # resample dataset using the max value for each 24-hours
df = df.rolling(120).mean().dropna()  # perform moving average smoothing

df.head(10)  # display data frame

rawData = df

# scale data
scaler = MinMaxScaler()
scaler.fit(df)
# train label scaler
label_scaler = MinMaxScaler()
label_scaler.fit(df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3']])

scaled_ds = scaler.transform(df)
df = pd.DataFrame(scaled_ds, columns=df.columns, index=df.index)

#PCA AND EUCLIDEAN KERNEL

# center data
rainfall_df = df[['RF-Intensity', 'RF-Intensity.1', 'RF-Intensity.2', 'RF-Intensity.3']]


# calculate pairwise squared Euclidean distances
sq_dists = sc.spatial.distance.pdist(rainfall_df.values.T, 'sqeuclidean')

# convert pairwise distances into a square matrix
mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

# compute the symmetric kernel matrix.
gamma = 1 / len(rainfall_df.columns)
K = np.exp(-gamma * mat_sq_dists)

# center the kernel matrix.
N = K.shape[0]
one_n = np.ones((N, N)) / N
K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

# calculate eigenvectors and eigenvalues
eigenvalues, eigenvectors = np.linalg.eigh(K)

# calculate components
rainfall_df = np.matmul(rainfall_df, eigenvectors) 
rainfall_df = rainfall_df.iloc[:, 1]

# center data
precipitation_df = df[['Precipitation', 'Precipitation.1', 'Precipitation.2']]


# calculate pairwise squared Euclidean distances
sq_dists = sc.spatial.distance.pdist(precipitation_df.values.T, 'sqeuclidean')

# convert pairwise distances into a square matrix
mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

# compute the symmetric kernel matrix.
gamma = 1/len(precipitation_df.columns)
K = np.exp(-gamma * mat_sq_dists)

# center the kernel matrix.
N = K.shape[0]
one_n = np.ones((N, N)) / N
K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

# calculate eigenvectors and eigenvalues
eigenvalues, eigenvectors = np.linalg.eigh(K)

# calculate components
precipitation_df = np.matmul(precipitation_df, eigenvectors) 
precipitation_df = precipitation_df.iloc[:, 1]

# center data
humidity_df = df[['Humidity', 'Humidity.1', 'Humidity.2']]



# calculate pairwise squared Euclidean distances
sq_dists = sc.spatial.distance.pdist(humidity_df.values.T, 'sqeuclidean')

# convert pairwise distances into a square matrix
mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

# compute the symmetric kernel matrix.
gamma = 1/len(humidity_df.columns)
K = np.exp(-gamma * mat_sq_dists)

# center the kernel matrix.
N = K.shape[0]
one_n = np.ones((N, N)) / N
K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

# calculate eigenvectors and eigenvalues
eigenvalues, eigenvectors = np.linalg.eigh(K)

# calculate components
humidity_df = np.matmul(humidity_df, eigenvectors) 
humidity_df = humidity_df.iloc[:, 1]

# center data
temp_df = df[['Temperature', 'Temperature.1', 'Temperature.2']]



# calculate pairwise squared Euclidean distances
sq_dists = sc.spatial.distance.pdist(temp_df.values.T, 'sqeuclidean')

# convert pairwise distances into a square matrix
mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

# compute the symmetric kernel matrix.
gamma = 1/len(temp_df.columns)
K = np.exp(-gamma * mat_sq_dists)

# center the kernel matrix.
N = K.shape[0]
one_n = np.ones((N, N)) / N
K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

# calculate eigenvectors and eigenvalues
eigenvalues, eigenvectors = np.linalg.eigh(K)

# calculate components
temp_df = np.matmul(temp_df, eigenvectors)
temp_df = temp_df.iloc[:, 1]

weather_df = pd.concat([rainfall_df, precipitation_df, humidity_df, temp_df], axis=1)
weather_df.columns = ['Rainfall', 'Precipitation', 'Humidity', 'Temperature']

river_df = df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3']]
reduced_df = pd.concat([river_df, weather_df], axis=1)

print(reduced_df.head(10))


cleanData = reduced_df



class TimeSeriesDataset(torch.utils.data.Dataset):
    def __init__(self, data, seq_len, step):
        self.data = data
        self.seq_len = seq_len
        self.step = step
        
    def __getitem__(self, index):
        in_start = index
        in_end = in_start + self.seq_len
        out_start = index + self.step
        out_end = out_start + self.seq_len
        
        inputs = self.data[in_start:in_end]
        labels = self.data[out_start:out_end]
        
        return inputs, labels
    
    def __len__(self):
        return len(self.data) - (self.seq_len + self.step) + 1

BATCH_SIZE = 128
SEQ_LEN = 180
SEQ_STEP = 60
PRED_SIZE = 8
D_MODEL = 8
NUM_HEADS = 4
NUM_LAYERS = 2
D_FF = 2048 
DROPOUT = 0.10

class MultiHeadAttention(nn.Module):
    def __init__(self, d_model, num_heads):
        super(MultiHeadAttention, self).__init__()
        assert d_model % num_heads == 0, "d_model must be divisible by num_heads"
        
        self.d_model = d_model
        self.num_heads = num_heads
        self.d_k = d_model // num_heads
        
        self.W_q = nn.Linear(d_model, d_model)
        self.W_k = nn.Linear(d_model, d_model)
        self.W_v = nn.Linear(d_model, d_model)
        self.W_o = nn.Linear(d_model, d_model)
        
    def scaled_dot_product_attention(self, Q, K, V, mask=None):
        attn_scores = torch.matmul(Q, K.transpose(-2, -1)) / math.sqrt(self.d_k)
        attn_scores = attn_scores.masked_fill(mask == 0, -1e9)
            
        attn_probs = torch.softmax(attn_scores, dim=-1)
        output = torch.matmul(attn_probs, V)
        
        return attn_probs, output
        
    def split_heads(self, x):
        batch_size, seq_length, d_model = x.size()
        return x.view(batch_size, seq_length, self.num_heads, self.d_k).transpose(1, 2)
        
    def combine_heads(self, x):
        batch_size, _, seq_length, d_k = x.size()
        return x.transpose(1, 2).contiguous().view(batch_size, seq_length, self.d_model)
        
    def forward(self, Q, K, V, mask=None):
        Q = self.split_heads(self.W_q(Q))
        K = self.split_heads(self.W_k(K))
        V = self.split_heads(self.W_v(V))
        
        attn_scores, attn_output = self.scaled_dot_product_attention(Q, K, V, mask)
        output = self.W_o(self.combine_heads(attn_output))
        return attn_scores, output
    
class PositionalEncoding(nn.Module):
    def __init__(self, d_model, max_seq_length=2048):
        super(PositionalEncoding, self).__init__()
        
        pe = torch.zeros(max_seq_length, d_model)
        position = torch.arange(0, max_seq_length, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * -(math.log(10000.0) / d_model))
        
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        
        self.register_buffer('pe', pe.unsqueeze(0))
        
    def forward(self, x):
        return x + self.pe[:, :x.size(1)]
    
class PositionWiseFeedForward(nn.Module):
    def __init__(self, d_model, d_ff):
        super(PositionWiseFeedForward, self).__init__()
        self.fc1 = nn.Linear(d_model, d_ff)
        self.fc2 = nn.Linear(d_ff, d_model)
        self.relu = nn.ReLU()

    def forward(self, x):
        return self.fc2(self.relu(self.fc1(x)))
    
class DecoderLayer(nn.Module):
    def __init__(self, d_model, num_heads, d_ff, dropout):
        super(DecoderLayer, self).__init__()
        self.self_attn = MultiHeadAttention(d_model, num_heads)
        self.feed_forward = PositionWiseFeedForward(d_model, d_ff)
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)
        self.dropout = nn.Dropout(dropout)
        
    def forward(self, x, mask=None):
        attn_scores, attn_output = self.self_attn(x, x, x, mask)
        x = self.norm1(x + self.dropout(attn_output))
        ff_output = self.feed_forward(x)
        x = self.norm2(x + self.dropout(ff_output))
        return attn_scores, x
    
class Transformer(nn.Module):
    def __init__(self, pred_size, d_model, num_heads, num_layers, d_ff, dropout):
        super(Transformer, self).__init__()
        self.positional_encoding = PositionalEncoding(d_model)
        self.decoder_layers = nn.ModuleList([DecoderLayer(d_model, num_heads, d_ff, dropout) for _ in range(num_layers)])
        self.fc = nn.Linear(d_model, pred_size)
        self.sigmoid = nn.Sigmoid()
        self.dropout = nn.Dropout(dropout)
        
    def generate_mask(self, tgt):
        seq_length = tgt.size(1)
        tgt_mask = (1 - torch.triu(torch.ones(1, seq_length, seq_length), diagonal=1)).bool()
        return tgt_mask

    def forward(self, tgt):
        mask = self.generate_mask(tgt).to(device)
        tgt_embedded = self.dropout(self.positional_encoding(tgt))

        dec_output = tgt_embedded
        for dec_layer in self.decoder_layers:
            attn_scores, dec_output = dec_layer(dec_output, mask)

        output = self.sigmoid(self.fc(dec_output))
        return attn_scores, output

# define the model
decomposer = Transformer(
    pred_size=PRED_SIZE,
    d_model=D_MODEL,
    num_heads=NUM_HEADS,
    num_layers=NUM_LAYERS,
    d_ff=D_FF,
    dropout=DROPOUT
).float()

decomposer.to(device)

decomposer.load_state_dict(torch.load('transformer.pth'))

decomposer.eval()  # set model on test mode

def forecast():
    test_data = reduced_df['2023-09-25':].values
    test_dates = reduced_df['2023-09-25':].index
    test_dates = test_dates[60:240]

    x_test = test_data[:180]
    y_label = test_data[60:]
    y_label = label_scaler.inverse_transform(y_label[:, :4])

    x_test = np.reshape(x_test, (1, x_test.shape[0], x_test.shape[1]))

    decomposer.eval()  # set model on test mode

    x_test = torch.from_numpy(x_test).float().to(device)
    attn_scores, y_test = decomposer(x_test)  # make forecast
    y_test = y_test.detach().cpu().numpy()
    y_test = np.reshape(y_test, (y_test.shape[1], y_test.shape[2]))
    y_test = label_scaler.inverse_transform(y_test[:, :4])


    time_steps_per_day = 4  # Assuming 4 time steps per day (6 hours per time step)
    forecast_days = 15

    # Extract the forecast for the next 15 days
    forecast_values = y_test[:forecast_days * time_steps_per_day]

    # Create a DataFrame with the forecasted values and dates
    forecast_dates = pd.date_range(test_dates[-1], periods=forecast_days * time_steps_per_day + 1, freq='6H')[1:]
    forecast_df = pd.DataFrame(data=forecast_values, columns=['P.Waterlevel', 'P.Waterlevel-1', 'P.Waterlevel-2', 'P.Waterlevel-3'])
    forecast_df.insert(0, "DateTime", forecast_dates)

    # Extract the forecast for the next 15 days
    true_values = y_label[-1:]

    true_dates = pd.date_range(start= test_dates[-1] , end= test_dates[-1] )[:]
    true_df = pd.DataFrame(data=true_values ,columns=['T.Waterlevel', 'T.Waterlevel-1', 'T.Waterlevel-2', 'T.Waterlevel-3']) #converting numpy to dataframe
    true_df.insert(0, "DateTime", true_dates) #adding DateTime column


    return forecast_df, true_df




def sendCleanData():
    df = pd.read_csv('riverlevel.csv')  # import dataset

    # convert month name to integer
    month_dict = dict((v, k) for k, v in enumerate(calendar.month_name))
    df['Month'] = df['Month'].map(month_dict)

    # create datetime column
    df[['Year', 'Month', 'Day', 'Hour']] = df[['Year', 'Month', 'Day', 'Hour']].astype(int)
    df['Hour'] = df['Hour'].apply(lambda x: x if x < 24 else 0)

    # convert year, month, day, and hour columns into timestamp
    df['Datetime'] = df[['Year', 'Month', 'Day', 'Hour']].apply(lambda row: datetime(row['Year'], row['Month'], row['Day'], row['Hour']).isoformat(), axis=1)
    df["Datetime"] = pd.to_datetime(df["Datetime"], format='ISO8601')

    # assign timestamps as the data frame index
    df.index = df["Datetime"]
    df = df.drop(['Datetime'], axis=1)

    # select the parameters
    df = df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3', 'RF-Intensity', 'RF-Intensity.1', 'RF-Intensity.2', 'RF-Intensity.3', 'Precipitation', 'Precipitation.1', 'Precipitation.2', 'Humidity', 'Humidity.1', 'Humidity.2', 'Temperature', 'Temperature.1', 'Temperature.2']] 
    df = df.astype(np.float64)  # convert parameters into a double precision floating number

    # fill in missing values using linear interpolation
    df = df.interpolate(method='linear', limit_direction='forward')
    df = df.resample('6H').max()  # resample dataset using the max value for each 24-hours
    df = df.rolling(120).mean().dropna()  # perform moving average smoothing

    df.head(10)  # display data frame

    rawData = df

    # scale data
    scaler = MinMaxScaler()
    scaler.fit(df)
    # train label scaler
    label_scaler = MinMaxScaler()
    label_scaler.fit(df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3']])

    scaled_ds = scaler.transform(df)
    df = pd.DataFrame(scaled_ds, columns=df.columns, index=df.index)

    #PCA AND EUCLIDEAN KERNEL

    # center data
    rainfall_df = df[['RF-Intensity', 'RF-Intensity.1', 'RF-Intensity.2', 'RF-Intensity.3']]


    # calculate pairwise squared Euclidean distances
    sq_dists = sc.spatial.distance.pdist(rainfall_df.values.T, 'sqeuclidean')

    # convert pairwise distances into a square matrix
    mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

    # compute the symmetric kernel matrix.
    gamma = 1 / len(rainfall_df.columns)
    K = np.exp(-gamma * mat_sq_dists)

    # center the kernel matrix.
    N = K.shape[0]
    one_n = np.ones((N, N)) / N
    K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

    # calculate eigenvectors and eigenvalues
    eigenvalues, eigenvectors = np.linalg.eigh(K)

    # calculate components
    rainfall_df = np.matmul(rainfall_df, eigenvectors) 
    rainfall_df = rainfall_df.iloc[:, 1]

    # center data
    precipitation_df = df[['Precipitation', 'Precipitation.1', 'Precipitation.2']]


    # calculate pairwise squared Euclidean distances
    sq_dists = sc.spatial.distance.pdist(precipitation_df.values.T, 'sqeuclidean')

    # convert pairwise distances into a square matrix
    mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

    # compute the symmetric kernel matrix.
    gamma = 1/len(precipitation_df.columns)
    K = np.exp(-gamma * mat_sq_dists)

    # center the kernel matrix.
    N = K.shape[0]
    one_n = np.ones((N, N)) / N
    K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

    # calculate eigenvectors and eigenvalues
    eigenvalues, eigenvectors = np.linalg.eigh(K)

    # calculate components
    precipitation_df = np.matmul(precipitation_df, eigenvectors) 
    precipitation_df = precipitation_df.iloc[:, 1]

    # center data
    humidity_df = df[['Humidity', 'Humidity.1', 'Humidity.2']]



    # calculate pairwise squared Euclidean distances
    sq_dists = sc.spatial.distance.pdist(humidity_df.values.T, 'sqeuclidean')

    # convert pairwise distances into a square matrix
    mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

    # compute the symmetric kernel matrix.
    gamma = 1/len(humidity_df.columns)
    K = np.exp(-gamma * mat_sq_dists)

    # center the kernel matrix.
    N = K.shape[0]
    one_n = np.ones((N, N)) / N
    K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

    # calculate eigenvectors and eigenvalues
    eigenvalues, eigenvectors = np.linalg.eigh(K)

    # calculate components
    humidity_df = np.matmul(humidity_df, eigenvectors) 
    humidity_df = humidity_df.iloc[:, 1]

    # center data
    temp_df = df[['Temperature', 'Temperature.1', 'Temperature.2']]



    # calculate pairwise squared Euclidean distances
    sq_dists = sc.spatial.distance.pdist(temp_df.values.T, 'sqeuclidean')

    # convert pairwise distances into a square matrix
    mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

    # compute the symmetric kernel matrix.
    gamma = 1/len(temp_df.columns)
    K = np.exp(-gamma * mat_sq_dists)

    # center the kernel matrix.
    N = K.shape[0]
    one_n = np.ones((N, N)) / N
    K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

    # calculate eigenvectors and eigenvalues
    eigenvalues, eigenvectors = np.linalg.eigh(K)

    # calculate components
    temp_df = np.matmul(temp_df, eigenvectors)
    temp_df = temp_df.iloc[:, 1]

    weather_df = pd.concat([rainfall_df, precipitation_df, humidity_df, temp_df], axis=1)
    weather_df.columns = ['Rainfall', 'Precipitation', 'Humidity', 'Temperature']

    river_df = df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3']]
    reduced_df = pd.concat([river_df, weather_df], axis=1)

    print(reduced_df.head(10))


    cleanData = reduced_df

    return cleanData


def sendrawData():
    df = pd.read_csv('riverlevel.csv')  # import dataset

    # convert month name to integer
    month_dict = dict((v, k) for k, v in enumerate(calendar.month_name))
    df['Month'] = df['Month'].map(month_dict)

    # create datetime column
    df[['Year', 'Month', 'Day', 'Hour']] = df[['Year', 'Month', 'Day', 'Hour']].astype(int)
    df['Hour'] = df['Hour'].apply(lambda x: x if x < 24 else 0)

    # convert year, month, day, and hour columns into timestamp
    df['Datetime'] = df[['Year', 'Month', 'Day', 'Hour']].apply(lambda row: datetime(row['Year'], row['Month'], row['Day'], row['Hour']).isoformat(), axis=1)
    df["Datetime"] = pd.to_datetime(df["Datetime"], format='ISO8601')

    # assign timestamps as the data frame index
    df.index = df["Datetime"]
    df = df.drop(['Datetime'], axis=1)

    # select the parameters
    df = df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3', 'RF-Intensity', 'RF-Intensity.1', 'RF-Intensity.2', 'RF-Intensity.3', 'Precipitation', 'Precipitation.1', 'Precipitation.2', 'Humidity', 'Humidity.1', 'Humidity.2', 'Temperature', 'Temperature.1', 'Temperature.2']] 
    df = df.astype(np.float64)  # convert parameters into a double precision floating number

    # fill in missing values using linear interpolation
    df = df.interpolate(method='linear', limit_direction='forward')
    df = df.resample('6H').max()  # resample dataset using the max value for each 24-hours
    df = df.rolling(120).mean().dropna()  # perform moving average smoothing

    df.head(10)  # display data frame

    rawData = df

    # scale data
    scaler = MinMaxScaler()
    scaler.fit(df)
    # train label scaler
    label_scaler = MinMaxScaler()
    label_scaler.fit(df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3']])

    scaled_ds = scaler.transform(df)
    df = pd.DataFrame(scaled_ds, columns=df.columns, index=df.index)

    #PCA AND EUCLIDEAN KERNEL

    # center data
    rainfall_df = df[['RF-Intensity', 'RF-Intensity.1', 'RF-Intensity.2', 'RF-Intensity.3']]


    # calculate pairwise squared Euclidean distances
    sq_dists = sc.spatial.distance.pdist(rainfall_df.values.T, 'sqeuclidean')

    # convert pairwise distances into a square matrix
    mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

    # compute the symmetric kernel matrix.
    gamma = 1 / len(rainfall_df.columns)
    K = np.exp(-gamma * mat_sq_dists)

    # center the kernel matrix.
    N = K.shape[0]
    one_n = np.ones((N, N)) / N
    K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

    # calculate eigenvectors and eigenvalues
    eigenvalues, eigenvectors = np.linalg.eigh(K)

    # calculate components
    rainfall_df = np.matmul(rainfall_df, eigenvectors) 
    rainfall_df = rainfall_df.iloc[:, 1]

    # center data
    precipitation_df = df[['Precipitation', 'Precipitation.1', 'Precipitation.2']]


    # calculate pairwise squared Euclidean distances
    sq_dists = sc.spatial.distance.pdist(precipitation_df.values.T, 'sqeuclidean')

    # convert pairwise distances into a square matrix
    mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

    # compute the symmetric kernel matrix.
    gamma = 1/len(precipitation_df.columns)
    K = np.exp(-gamma * mat_sq_dists)

    # center the kernel matrix.
    N = K.shape[0]
    one_n = np.ones((N, N)) / N
    K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

    # calculate eigenvectors and eigenvalues
    eigenvalues, eigenvectors = np.linalg.eigh(K)

    # calculate components
    precipitation_df = np.matmul(precipitation_df, eigenvectors) 
    precipitation_df = precipitation_df.iloc[:, 1]

    # center data
    humidity_df = df[['Humidity', 'Humidity.1', 'Humidity.2']]



    # calculate pairwise squared Euclidean distances
    sq_dists = sc.spatial.distance.pdist(humidity_df.values.T, 'sqeuclidean')

    # convert pairwise distances into a square matrix
    mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

    # compute the symmetric kernel matrix.
    gamma = 1/len(humidity_df.columns)
    K = np.exp(-gamma * mat_sq_dists)

    # center the kernel matrix.
    N = K.shape[0]
    one_n = np.ones((N, N)) / N
    K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

    # calculate eigenvectors and eigenvalues
    eigenvalues, eigenvectors = np.linalg.eigh(K)

    # calculate components
    humidity_df = np.matmul(humidity_df, eigenvectors) 
    humidity_df = humidity_df.iloc[:, 1]

    # center data
    temp_df = df[['Temperature', 'Temperature.1', 'Temperature.2']]



    # calculate pairwise squared Euclidean distances
    sq_dists = sc.spatial.distance.pdist(temp_df.values.T, 'sqeuclidean')

    # convert pairwise distances into a square matrix
    mat_sq_dists = sc.spatial.distance.squareform(sq_dists)

    # compute the symmetric kernel matrix.
    gamma = 1/len(temp_df.columns)
    K = np.exp(-gamma * mat_sq_dists)

    # center the kernel matrix.
    N = K.shape[0]
    one_n = np.ones((N, N)) / N
    K = K - one_n.dot(K) - K.dot(one_n) + one_n.dot(K).dot(one_n)

    # calculate eigenvectors and eigenvalues
    eigenvalues, eigenvectors = np.linalg.eigh(K)

    # calculate components
    temp_df = np.matmul(temp_df, eigenvectors)
    temp_df = temp_df.iloc[:, 1]

    weather_df = pd.concat([rainfall_df, precipitation_df, humidity_df, temp_df], axis=1)
    weather_df.columns = ['Rainfall', 'Precipitation', 'Humidity', 'Temperature']

    river_df = df[['Waterlevel', 'Waterlevel.1', 'Waterlevel.2', 'Waterlevel.3']]
    reduced_df = pd.concat([river_df, weather_df], axis=1)

    print(reduced_df.head(10))


    cleanData = reduced_df

    return rawData
