import pandas as pd
from datetime import datetime as dt
import copy


# tweets_path='D:/GeoJob/geoepi/1_Data_Retrieval/RJ_City/CSD/Twitter/twitter_paper/data/pois_df.csv'
# tweets_df = pd.read_csv(tweets_path).drop(columns=['withheld'])


# def load_salzburg(timestamp=False):
#     tweets_df = pd.read_csv("D:/GeoJob/geoepi/1_Data_Retrieval/RJ_City/CSD/Twitter/twitter_paper/data/salzburg_wir_cleaned.csv")
       
    
#     return tweets_df

def load_and_subset(start, end, del_one_tweeters=True, timestamp=False, tweets_path='data/tweets/all_data_prepped.csv', samp_size=False):
    tweets_df = pd.read_csv(tweets_path)
    if 'withheld' in tweets_df.columns:
        tweets_df.drop(columns=['withheld'])
    
    tweets_df['Timestamp'] = tweets_df['Timestamp'].apply(lambda x: dt.strptime(x, "%Y-%m-%d %H:%M:%S"))
    
    s_date = dt.strptime(start, "%Y%m%d")
    e_date = dt.strptime(end, "%Y%m%d")
    
    if not timestamp:
        tweets_df.Timestamp = tweets_df.Timestamp.apply(dt.date)
        s_date = s_date.date()
        e_date = e_date.date()


    
    tweets_df = tweets_df[(tweets_df.Timestamp < e_date) & (tweets_df.Timestamp >= s_date)]
    
    if del_one_tweeters:
        a = tweets_df.groupby('User_ID').count()
        b = a[a.Timestamp > 1]
        tweets_df = tweets_df[tweets_df.User_ID.isin(b.index.values)]
        
    if type(samp_size) == int:
        if samp_size < len(tweets_df):
            tweets_df = tweets_df.sample(samp_size)
            
        
    return tweets_df


def load_subset_multiple(starts, ends, del_one_tweeters=False, timestamp=False, tweets_path='data/tweets/all_data_prepped.csv', samp_size=False):
    if type(starts) != list or type(ends) != list:
        raise TypeError("Please give starts and ends as list in order to user this function!")
        
    tweets_df = pd.read_csv(tweets_path)
        
    if 'withheld' in tweets_df.columns:
        tweets_df = tweets_df.drop(columns=['withheld'])

        
    tweets_df['Timestamp'] = tweets_df['Timestamp'].apply(lambda x: dt.strptime(x, "%Y-%m-%d %H:%M:%S"))
    
    if not timestamp:
        tweets_df.Timestamp = tweets_df.Timestamp.apply(dt.date)
        
    tweets_dfs = []

    
    
    ##############################
    # LOOP #
    ##############################
    
    for start, end in zip(starts, ends):
        print(f'getting subset {start} to {end}')
        
        df = copy.deepcopy(tweets_df)
    
        s_date = dt.strptime(start, "%Y%m%d")
        e_date = dt.strptime(end, "%Y%m%d")
        
        if not timestamp:
            s_date = s_date.date()
            e_date = e_date.date()
            
        df = df[(df.Timestamp < e_date) & (df.Timestamp >= s_date)]
        
        if del_one_tweeters:
            a = df.groupby('User_ID').count()
            b = a[a.Timestamp > 1]
            df = df[df.User_ID.isin(b.index.values)]
            
        if type(samp_size) == int:
            if samp_size < len(df):
                df = df.sample(samp_size)
                
        tweets_dfs.append(df)
        
    return tweets_dfs
                    
# start = "20200405"
# end = "20201031"

# salz = load_and_subset(start, end, tweets_path='../data/salzburg_wir_cleaned.csv')

# tweets_df = load_and_subset(start, end, tweets_path='../data/pois_df.csv')