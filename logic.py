from datetime import datetime as dt

import pandas as pd
import time as tm

TIME_ZONE = 'Asia/Dubai'

def accept_message(message):
    to_filter = ['has joined the channel', 'uploaded a file', '?', '#', '@', '(', ')']
    def is_english(message):
        try:
            message.encode(encoding='utf-8').decode('ascii')
        except UnicodeDecodeError:
            return False
        else:
            return True
    return not any(x in message[0] for x in to_filter) and not is_english(message[0])

def clean_message(message):
    return message.strip().replace('!', '')
    
def preprocess_messages(messages, max_length=20):
    messages = [(clean_message(message['text']),message['ts']) 
                for message in messages if message['type'] == 'message']
    messages = [message for message in messages if accept_message(message)]
    split_messages_new_line = []
    for message in messages:
        for _message in message[0].split('\n'):
            split_messages_new_line.append((_message, message[1]))
    split_messages_comma = []    
    for message in split_messages_new_line:
        for _message in message[0].split(','):
            split_messages_comma.append((_message, message[1]))
    messages = [message for message in split_messages_comma if len(message[0]) < max_length]
    return messages

def get_restaurants_and_timestamps(messages):
    return preprocess_messages(messages)

def normalize_timestamps(restaurants_and_timestamps):
    updated_restaurants_and_timestamps = []
    for restaurant_and_timestamp in restaurants_and_timestamps:
        old_epoch = int(str(restaurant_and_timestamp[1].split('.')[0]))
        new_epoch = tm.mktime(dt.fromtimestamp(old_epoch).replace(year=1983, month=12, day=9).timetuple())
        updated_restaurants_and_timestamps.append((restaurant_and_timestamp[0], new_epoch))
    return updated_restaurants_and_timestamps

def create_dataframe(messages):
    restaurants_and_timestamps = normalize_timestamps(get_restaurants_and_timestamps(messages=messages))
    return pd.DataFrame(restaurants_and_timestamps, columns=['restaurant', 'timestamp'])
    
def process_dataframe(dataframe, min_threshold=4):    
    dataframe = dataframe.groupby('restaurant')     .agg({'restaurant':'count', 'timestamp':'mean'})\
        .rename(columns={'restaurant':'count','timestamp':'mean'})\
        .reset_index()
    dataframe['mean_hour'] = pd.to_datetime(dataframe['mean'], unit='s')
    dataframe['mean_hour'] = dataframe['mean_hour'].dt.tz_localize('UTC').dt.tz_convert(TIME_ZONE)
    return dataframe[dataframe['count'] >= min_threshold]

def print_dataframe(dataframe, column):
    dataframe['mean_hour'] = dataframe['mean_hour'].dt.strftime('%H:%M')
    result = ''
    for _, row in dataframe.iterrows():
        result += row['restaurant'] + '\t' + str(row[column]) + '\n'
    return result

def get_n_most_popular_restaurants(dataframe, n=10):
    restaurants = dataframe.sort_values('count', ascending=False).head(n)
    restaurants['mean_hour'] = restaurants['mean_hour'].dt.strftime('%H:%M')
    result = ''
    for _, row in restaurants.iterrows():
        result += row['restaurant'] + '\n'
    return result

def get_n_earliest_restaurants(dataframe, n=10):
    restaurants = dataframe.sort_values('mean_hour', ascending=True).head(n)
    return print_dataframe(dataframe=restaurants, column='mean_hour')

def get_n_latest_restaurants(dataframe, n=10):
    restaurants = dataframe.sort_values('mean_hour', ascending=False).head(n)
    return print_dataframe(dataframe=restaurants, column='mean_hour')

def filter_restaurants_between_hours(dataframe, start_hour, end_hour):
    import pytz
    local_tz = pytz.timezone(TIME_ZONE)
    start = local_tz.localize(dt.strptime('1983-12-09 ' + start_hour, "%Y-%m-%d %H:%M"))
    end = local_tz.localize(dt.strptime('1983-12-09 ' + end_hour, "%Y-%m-%d %H:%M"))
    return dataframe[(dataframe['mean_hour'] >= start) & (dataframe['mean_hour'] < end)]

def get_restaurants_by_average_time(dataframe, start_hour='09:00', end_hour='17:00', n=20):
    restaurants = filter_restaurants_between_hours(dataframe=dataframe, start_hour=start_hour, end_hour=end_hour)
    restaurants = restaurants.sort_values('mean_hour', ascending=True).head(n)
    return print_dataframe(dataframe=restaurants, column='mean_hour')

def get_average_time_for_restaurant(dataframe, restaurant):
    result = dataframe.loc[dataframe['restaurant'] == restaurant]['mean_hour']
    if result.size == 0:
        return ''
    return restaurant + '\t' + str(result.dt.strftime('%H:%M').values[0])