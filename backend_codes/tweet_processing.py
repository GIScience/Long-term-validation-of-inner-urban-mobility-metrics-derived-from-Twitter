# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 10:56:28 2021

@author: Simon Groß
"""

import copy
import html
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime
import matplotlib.pyplot as plt
from shapely.geometry import Polygon

test_tweet = [{'conversation_id': '1344793695216930827', 'created_at': '2020-12-31T23:52:56.000Z', 'id': '1344793695216930827', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 1, 'quote_count': 0}, 'text': 'Und wenn ich morgen wieder wach bin, will ich Schnee sehen! ❄️⛄', 'author_id': {'id': '949958840426131456', 'name': 'Osterbob', 'username': 'Pyjamaheld'}, 'lang': 'de'}]

class FullResponseBuilder():
    def __init__(self, tweets):
        self.tweets = tweets
        
        self.tweets_original = tweets
        self.full_response = {'data': [],
                              'includes': {
                                  'places': [],
                                  'users': []
                                  },
                              'errors': []
                              }
        
    def initiate(self):
        self.delete_from_tweets('users')
        self.delete_from_tweets('places')
        
        self.full_response['includes']['places'].extend(self.places)
        #self.full_response['includes']['users'].extend(self.users)
        self.full_response['data'] = copy.deepcopy(self.tweets)
        
        return self.full_response
        
    def delete_from_tweets(self, _type):
        
        _length = len(self.tweets)
        
        if _type == 'places':
            place_id_list = []
            self.places = []
            
            for i, tweet in enumerate(self.tweets):
                if i % (_length // 10) == 0:
                    print(f'{(i/_length)*100} % of place attachments finished!')
                if 'geo' not in tweet:
                    continue
                if 'coordinates' in tweet['geo']:
                    continue
                if len(tweet['geo']) == 1:
                    # für places, die nicht gefunden wurden und deshalb nur die ID sind
                    continue
                
                if tweet['geo']['id'] not in place_id_list:
                    self.places.append(tweet['geo'])
                    place_id_list.append(tweet['geo']['id'])
                
                tweet['geo'] = {'place_id': tweet['geo']['id']}
                
                
        if _type == 'users':
            
            for i, tweet in enumerate(self.tweets):
                if i % (_length // 10) == 0:
                    print(f'{(i/_length)*100} % of user attachments finished!')

                tweet['author_id'] = tweet['author_id']['id']
            
class PlaceAnalyser():
    def __init__(self, places):
        self.places_original = copy.deepcopy(places)
        self.places = places
        
    def reset_analyses(self):
        self.places = self.places_original
        print('Reset sucessfull')
        return self.places
    
    def use_filters(self, cities=True, cities_or_bigger=True,
                    neighborhoods=False, pois=False):
        
        if cities_or_bigger:
            self.filter_cities_and_bigger()
            print('\n')
            
        if cities and not cities_or_bigger:
            self.filter_cities()
            print('\n')
            
        if pois:
            self.only_pois()
            print('\n')
            
        if neighborhoods:
            self.only_neighborhoods()
            print('\n')
            
        return self.places
    
    def only_pois(self):
        original_length = len(self.places)
        self.pois = self._only(_type='poi')
        
        print(f'Removed {original_length - len(self.pois)} places')
        print(f'{(len(self.pois)/original_length)*100} % of the places were POIs ')
        
    def only_neighborhoods(self):
        original_length = len(self.places)
        self.neighborhoods = self._only(_type = 'neighborhood')
        
        print(f'Removed {original_length - len(self.neighborhoods)} places')
        print(f'{(len(self.neighborhoods)/original_length)*100} % of the places were Neighborhoods ')
        
    def _only(self, _type):
        black_list = []
        original_length = len(self.places)
                
        _temp = [place for i, place in enumerate(self.places) if place['place_type'] == _type]
         
        return _temp
    
    def filter_cities(self):
        original_length = len(self.places)
        self.places = self._filter_place_types(['city'])
        
        print(f'Removed {original_length - len(self.places)} places')
        print(f'{100 - (len(self.places)/original_length)*100} % of the places were located in a city')
        
        return self.places
        
    def filter_cities_and_bigger(self):
        original_length = len(self.places)
        self.places = self._filter_place_types(['city', 'country', 'admin'])
        
        print(f'Removed {original_length - len(self.places)} Places')
        print(f'{100 - (len(self.places)/original_length)*100} % of the places were located in a city or bigger region')
        
        return self.places
        
    def _filter_place_types(self, types_list):
        _places = [place for place in self.places if place['place_type'] not in types_list]
        
        return _places
    
    def save_shapefile_bbox(self, filename, projection_str='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'):
        places = copy.deepcopy(self.places)
        
        gdf = self._make_geodataframe(places, shape_type='bbox')
        gdf.to_file(filename=filename, driver='ESRI Shapefile', crs_wkt=projection_str)

        return gdf
    
    def save_shapefile_centroid(self, filename, projection_str='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'):
        places = copy.deepcopy(self.places)
        
        gdf = self._make_geodataframe(places, shape_type='centroid')
        gdf.to_file(filename=filename, driver='ESRI Shapefile', crs_wkt=projection_str)

        return gdf
    
    def _make_geodataframe(self, places, shape_type):
        valid_types = ['centroid', 'bbox']
        if shape_type not in valid_types:
            raise KeyError('Shape type "{shape_type}" is not valid!')
            
        original_length = len(places)
        
        places = self._append_geometry(places, shape_type)
        df_places = self._make_dataframe(places, shape_type)
        
        print(f'Shapefile does contain {len(df_places)} of originally {original_length} places!')
        print(f'Location-rate of places: {int(len(df_places)/original_length*100)} %')
        
        if shape_type == 'centroid':
            places_gdf = gpd.GeoDataFrame(df_places, 
                                          geometry = gpd.points_from_xy(df_places['Lon'], df_places['Lat']))
         
        if shape_type == 'bbox':
            series = gpd.GeoSeries(df_places['Bounding_Box'])
            df_places = df_places.drop(columns=['Bounding_Box'])
            places_gdf = gpd.GeoDataFrame(df_places, geometry=series)
            
        return places_gdf
    
    def _make_dataframe(self, places, shape_type):
        df = pd.DataFrame(data=np.array([place['id'] for place in places]), columns=['Id_place'])
        df['Place_Type'] = np.array([place['place_type'] if 'place_type' in place else None for place in places])
        
        if shape_type == 'bbox':
            # df['Place_Name'] = np.array([_func(place) for place in places])
            
            # def _func
            df['Place_Name'] = np.array([html.unescape(place['name']) if 'name' in place else 'No_Name' for place in places])
       
        if shape_type == 'centroid':
            df['Lat'] = np.array([place['Lat'] if 'Lat' in place else None for place in places])
            df['Lon'] = np.array([place['Lon'] if 'Lon' in place else None for place in places]) 
       
        if shape_type == 'bbox':
            df['Bounding_Box'] = np.array([place['Bounding_Box'] if 'Bounding_Box' in place else None for place in places])
        
        print('Created Dataframe sucessfully')
        return df
    
    def _append_geometry(self, places, shape_type):
        for i, place in enumerate(places):
            if 'geo' in place:
                if 'bbox' in place['geo']:
                    bbox = place['geo']['bbox']
                    polygon = self._get_polygon(bbox)
                    
                    if shape_type == 'centroid':
                        centroid = self._get_centroid(polygon)
                        places[i]['Lon'] = centroid[0]
                        places[i]['Lat'] = centroid[1]
                        
                    if shape_type == 'bbox':
                        places[i]['Bounding_Box'] = Polygon(polygon)
                        
                    places[i]['Name'] = str(place['full_name'].encode('latin-1', 'replace'))[2:-1]

        return places
    
    def _get_polygon(self, bbox):
        polygon = ((bbox[0], bbox[1]), (bbox[2], bbox[1]), (bbox[2], bbox[3]), (bbox[0], bbox[3]))
        return polygon
    
    def _get_centroid(self, polygon):
        x_list = [vertex [0] for vertex in polygon]
        y_list = [vertex [1] for vertex in polygon]
        
        _len = len(polygon)
        x = sum(x_list) / _len
        y = sum(y_list) / _len
        return(x, y)

class TweetAnalyzer():
    '''
    Analizing Tweets
    '''
    def __init__(self, tweets=test_tweet):
        """
        A List of Tweets

        """
        self.tweets_original = tweets
        self.tweets = tweets
        self.df = self.tweets_to_dataframe()
        
        self.bot_tweets = []
        
        self.bot_account_list = {}
        self.account_list = {}
        
        self.bot_tweets_by_account = []
        
    def reset_analyses(self):
        """
        Resets the self.tweet variable to its original state

        """
        self.tweets = self.tweets_original
        self.tweets_to_dataframe()
        print('Resetted analysis!')
        return self.tweets
    
    def get_tweets_in_timedelta(self, start, end):
        start = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.000Z")
        end = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S.000Z")
        
        tweets_time = []
        original_length = len(self.tweets)
        
        for tweet in self.tweets:
            if (start <= datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.000Z") <= end):
                tweets_time.append(tweet)
                
        self.tweets = copy.deepcopy(tweets_time)
        print(f'{(len(self.tweets)/original_length)*100} % of tweets were inside the time-boundaries')
        return self.tweets               
    
    def get_bots(self):
        """
        Get all bot-tweets

        """
        return self.bot_tweets
    
    def use_all_bot_filters(self):
        """
        Use all Bot filters at once

        Returns
        -------
        updated self.tweets

        """
        print('Deleting Bot Tweets...')
        self.tweets = self.identify_bot_tweets()
        
        print('Deleting Bot Accounts...')
        self.tweets = self.identify_bot_accounts()
        
        print('Deleting Duplicates...')
        self.tweets = self.eliminate_duplicates()
        
        return self.tweets
    
    def identify_bot_tweets(self):
        """
        Delete all tweets presumably made by bots

        Returns
        -------
        updated self.tweets

        """
        black_list = []
        original_length = len(self.tweets)
        
        for i, tweet in enumerate(self.tweets):
            text = tweet['text']
            
            if self._has_bot_symbols(text):
                    
                tweet['original_index'] = i
                self.bot_tweets.append(tweet)
                black_list.append(i)
        
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
            
        try:
            print(f'Identified {((original_length-len(self.tweets))/original_length)*100} % of Tweets as bot-Tweets')
        except:
            pass
        return self.tweets
    
    def identify_bot_accounts(self, threshold=0.005):
        """
        Identify bot-accounts by calculating their ratio of total tweets

        Returns
        -------
        The cleaned tweet-list.

        """
        original_length = len(self.tweets)
        for tweet in self.tweets:
            account_id = tweet['author_id']['id']
            
            if account_id in self.account_list:
                self.account_list[account_id] += 1
            else: 
                self.account_list[account_id] = 1
        
        black_list = []
        
        for account_key in self.account_list:
            ratio = self.account_list[account_key] / len(self.tweets_original)
            if ratio > threshold:
                self.bot_account_list[account_key + '_ratio'] = ratio
                black_list.extend(self._tweets_from_account(account_key))
                
        self.bot_tweets_by_account = [tweet for i, tweet in enumerate(self.tweets) if i in black_list]
                
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
        
        print(f'Deleted {(len(self.bot_tweets_by_account)/original_length)*100} % of tweets')
        return self.tweets
    
    def eliminate_duplicates_texts(self):
        """
        Too slow for big data
        
        Deletes all duplicate tweets (tweets with the same texts)

        Returns
        -------
        updated self.tweets

        """
        texts = []
        black_list = []
        original_length = len(self.tweets)
        
        for i, tweet in enumerate(self.tweets):
            if tweet['text'] in texts:
                black_list.append(i)
                
            texts.append(tweet['text'])
        
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
        
        print(f'Identified {(len(self.bot_tweets_by_account)/original_length)*100} % of tweets as Duplicates')
        
        return self.tweets

    def use_filters(self, retweets=True, place_type=True, geo=True, coordinates=True, cities=True,
                    cities_or_bigger=True, neighborhoods=False, pois=False):
        if retweets:
            self.filter_retweets()
            print('\n')
            
        if geo:
            self.filter_geo()
            print('\n')
            
        if coordinates:
            self.filter_coordinates()
            print('\n')
            
        if cities_or_bigger:
            self.filter_cities_and_bigger()
            print('\n')
            
        if cities and not cities_or_bigger:
            self.filter_cities()
            print('\n')
            
        if place_type:
            self.filter_no_place_type()
            print('\n')
            
        if pois:
            self.only_pois()
            print('\n')
            
        if neighborhoods:
            self.only_neighborhoods()
            print('\n')
            
        return self.tweets
    
    def only_pois(self):
        """
        Filters Tweets with no 'poi' tag
        
        """
        black_list = []
        original_length = len(self.tweets)
        for i, tweet in enumerate(self.tweets):
            if 'geo' not in tweet:
                black_list.append(i)
                continue
            
            if 'coordinates' in tweet['geo']:
                black_list.append(i)
                continue
            
            if 'place_type' not in tweet['geo']:
                black_list.append(i)
                print(f'No place type specified! Index: {i}')
                continue
            
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
                
        self.tweets = [tweet for i, tweet in enumerate(self.tweets) if tweet['geo']['place_type'] == 'poi']
        
        print(f'Removed {original_length - len(self.tweets)} Tweets')
        print(f'{(len(self.tweets)/original_length)*100} % of the Tweets were POIs ')
        
        self.tweets_to_dataframe()
        return self.tweets
        
    def only_neighborhoods(self):
        """
        Filters Tweets with no 'neighborhood' tag

        """
        black_list = []
        original_length = len(self.tweets)
        for i, tweet in enumerate(self.tweets):
            if 'geo' not in tweet:
                black_list.append(i)
                continue
            
            if 'coordinates' in tweet['geo']:
                black_list.append(i)
                continue
            
            if 'place_type' not in tweet['geo']:
                black_list.append(i)
                print(f'No place type specified! Index: {i}')
                continue
            
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
                
        self.tweets = [tweet for i, tweet in enumerate(self.tweets) if tweet['geo']['place_type'] == 'neighborhood']
        
        print(f'Removed {original_length - len(self.tweets)} Tweets')
        print(f'{(len(self.tweets)/original_length)*100} % of the Tweets were Neighborhoods ')
        
        self.tweets_to_dataframe()
        return self.tweets
    
    def filter_retweets(self):
        """
        Deletes all retweets

        """
        original_length = len(self.tweets)
        
        for i, tweet in enumerate(self.tweets):
            if 'referenced_tweets' in tweet:
                
                if len(tweet['referenced_tweets']) == 1 and tweet['referenced_tweets'][0]['type'] == 'retweet':
                    del self.tweets[i]
                    
        print(f'Deleted {original_length-len(self.tweets)} Tweets')
        print(f'{(len(self.tweets)/original_length)*100} % were not retweets')
        
        self.tweets_to_dataframe()
        return self.tweets
    
    def filter_no_place_type(self):
        """
        Filters all tweets with no place type specified.
        Does not filter non-geo tweets
        
        """
        original_length = len(self.tweets)        
        black_list = []
            
        for i, tweet in enumerate(self.tweets):
            if 'geo' in tweet:
                if 'place_type' not in tweet['geo']:
                    black_list.append(i)
                
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
        
        print(f'Removed {original_length - len(self.tweets)} Tweets')
        print(f'{(len(self.tweets)/original_length)*100} % of the Tweets have a place type attached')
        
        self.tweets_to_dataframe()
        return self.tweets
    
    def filter_geo(self):
        """
        Chooses only Tweets with a 'geo'-tag
        
        """
        black_list = []
        original_length = len(self.tweets)
        for i, tweet in enumerate(self.tweets):
            if 'geo' not in tweet:
                black_list.append(i)
        
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
        
        print(f'Removed {original_length - len(self.tweets)} Tweets')
        print(f'{(len(self.tweets)/original_length)*100} % of the Tweets have a location attached')
        
        self.tweets_to_dataframe()
        return self.tweets
    
    def filter_coordinates(self):
        """
        Filters all Tweets with coordinates as geo-specification.
        Does not filter non-geo tweets

        """
        black_list = []
        original_length = len(self.tweets)
        for i, tweet in enumerate(self.tweets):
            if 'geo' not in tweet:
                continue
            
            if 'coordinates' in tweet['geo']:
                black_list.append(i)
                
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
        
        print(f'Removed {original_length - len(self.tweets)} Tweets')
        print(f'{100 - (len(self.tweets)/original_length)*100} % of the Tweets were coordinates')
        
        self.tweets_to_dataframe()
        return self.tweets
    
    def filter_cities(self):
        """
        Filters all tweets with a 'city' tag.
        Does not filter non-geo tweets
        
        """
        black_list = []
        original_length = len(self.tweets)
        for i, tweet in enumerate(self.tweets):
            if 'geo' not in tweet:
                continue
            
            if 'coordinates' in tweet['geo']:
                continue
            
            if 'place_type' not in tweet['geo']:
                continue
            
            if tweet['geo']['place_type'] == 'city':
                black_list.append(i)
                
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
        
        print(f'Removed {original_length - len(self.tweets)} Tweets')
        print(f'{100 - (len(self.tweets)/original_length)*100} % of the Tweets were located in a city')
        
        self.tweets_to_dataframe()
        return self.tweets
    
    def filter_cities_and_bigger(self):
        """
        Filters all tweets with a 'city', 'admin' or 'country' tag.
        Does not filter non-geo objects

        """
        black_list = []
        original_length = len(self.tweets)
        for i, tweet in enumerate(self.tweets):
            if 'geo' not in tweet:
                continue
            
            if 'coordinates' in tweet['geo']:
                continue
            
            if 'place_type' not in tweet['geo']:
                continue
            
            if tweet['geo']['place_type'] == 'city' or tweet['geo']['place_type'] == 'country'\
                or tweet['geo']['place_type'] == 'admin':
                black_list.append(i)
                
        for i in sorted(black_list, reverse=True):
            del self.tweets[i]
        
        print(f'Removed {original_length - len(self.tweets)} Tweets')
        print(f'{100 - (len(self.tweets)/original_length)*100} % of the Tweets were located in a city or bigger region')
        
        self.tweets_to_dataframe()
        return self.tweets
           
    def tweets_to_dataframe(self):
        """
        Called in the contructor!
        
        The Tweet-List gets turned into a dataframe
        Information embedded into sub-objects (e.g. Likes/Retweets/etc. in the public_metrics sub object)
        is pulled out an given a single column

        Overwrites the self.df variable
        """
        df = pd.DataFrame(data=[html.unescape(tweet['text']) for tweet in self.tweets], columns=['Text'])
        df['Id_Tweet'] = np.array([tweet['id'] for tweet in self.tweets])
        df['Id_User'] = np.array([tweet['author_id']['id'] for tweet in self.tweets])
        df['Username'] = np.array([tweet['author_id']['username'] for tweet in self.tweets])
        df['Timestamp'] = np.array([tweet['created_at'] for tweet in self.tweets])
        df['Likes'] = np.array([tweet['public_metrics']['like_count'] for tweet in self.tweets])
        df['Retweets'] = np.array([tweet['public_metrics']['retweet_count'] for tweet in self.tweets])
        df['Quotes'] = np.array([tweet['public_metrics']['quote_count'] for tweet in self.tweets])
        df['Replies'] = np.array([tweet['public_metrics']['reply_count'] for tweet in self.tweets])
    
        df['Geo'] = np.array([tweet['geo'] if 'geo' in tweet else None for tweet in self.tweets])
        df['Place_Type'] = np.array([tweet['geo']['place_type'] if 'geo' in tweet and 'place_type' in tweet['geo'] else None for tweet in self.tweets])
        df['Place_Name'] = np.array([tweet['geo']['name'] if 'geo' in tweet and 'name' in tweet['geo'] else None for tweet in self.tweets])
        df['Id_Place'] = np.array([tweet['geo']['id'] if 'geo' in tweet and 'id' in tweet['geo'] else None for tweet in self.tweets])

        df['Referenced Tweets'] = np.array([tweet['referenced_tweets'] if 'referenced_tweets' in tweet else None for tweet in self.tweets], dtype=object)       
        
        df['Timestamp'] = df['Timestamp'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.000Z"))
        
        self.df = df
        
        return df
    
    def add_timedelta(self):
        self.df = self.df.sort_values(by=['Timestamp'])
        self.df['delta'] = (self.df['Timestamp']-self.df['Timestamp'].shift())
        
        return self.df
    
    def get_specific_tweet(self, _id):
        """
        Get a specific tweet from the objects tweets by specifying a tweet id

        Parameters
        ----------
        _id : string

        """
        for tweet in self.tweets:
            if tweet['id'] == _id:
                return tweet
            
        print('Tweet not found!')
    
    def get_tweets_exeptional_event(self):
        """
        Get all Tweets that were sent during a day with more than average tweets

        Returns
        -------
        tweets_exceptional : list
            A List of the Tweets.

        """
        days = self._get_exeptional_days()
        black_list = []
        
        for i, tweet in enumerate(self.tweets):
            tweet['created_at'] = datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.000Z").date()
            if tweet['created_at'] not in days:
                black_list.append(i)
                
        tweets_exceptional = [tweet for i, tweet in enumerate(self.tweets) if i not in black_list]
        return tweets_exceptional
    
    def get_most_interacted_accounts(self, sortby='Likes'):
        """
        The object-internal dataframe self.df gets grouped by account and sorted by a
        integer metric (e.g. Likes/Retweets/etc.)

        """
        df = self.df.groupby(by=['Username']).sum()
        df = df.sort_values(by=[sortby], ascending=False)
        return df

    def get_replies_tweet(self, tweet_id):
        """
        Returns all tweets that are a reply to the given tweet-id
        """
        return self._get_interaction_tweet(tweet_id, 'replied_to')
                        
    def get_quotes_tweet(self, tweet_id):
        """
        Returns all tweets that are a quote to the given tweet-id
        """
        return self._get_interaction_tweet(tweet_id, 'quoted')
    
    def get_retweets_tweet(self, tweet_id):
        """
        Returns all tweets that are a retweet to the given tweet-id
        """
        return self._get_interaction_tweet(tweet_id, 'retweeted')
    
    def save_df_csv(self, path):
        """
        Save the pandas Dataframe as csv at the given path: seperator is ";", index is False
        """
        self.df.to_csv(path, sep=';', index=False)
    
    def save_shapefile_bbox(self, filename, projection_str='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'):
        """
        Save each tweets' bounding box as a polygon in a shapefile

        tweet['geo']['geo']['bbox'] -> is the bounding Box of the tweets' loction
        
        The standard projection is EPSG:4326 - WGS84.
        It can be changed by specifying the respective porjection-string in th projection_str variable

        """
        tweets = copy.deepcopy(self.tweets)
        
        gdf = self._make_geodataframe(tweets, shape_type='bbox')
        gdf.to_file(filename=filename, driver='ESRI Shapefile', crs_wkt=projection_str)
    
    def save_shapefile_cooridnates(self, filename, projection_str='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'):
        """
        Save all tweets with coordinates as geo-specification in a point-layer shapefile

        The standard projection is EPSG:4326 - WGS84.
        It can be changed by specifying the respective porjection-string in th projection_str variable
        
        """
        tweets = copy.deepcopy(self.tweets)
        
        gdf = self._make_geodataframe(tweets, shape_type='coordinates')
        gdf.to_file(filename=filename, driver='ESRI Shapefile', crs_wkt=projection_str)
        
        return gdf
    
    def save_shapefile_centroid(self, filename, projection_str='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'):
        """
        Save self.tweets of the currrent analyser-object as a ESRI-Shapefile
        
        tweet['geo']['geo']['bbox'] -> is the bounding Box of the tweets' loction
        The bounding box will be converted to a point (the centroid)
        
        The projection is EPSG:4326 - WGS84
        It can be changed by specifying the respective porjection-string in th projection_str variable

        """
        
        tweets = copy.deepcopy(self.tweets)
        
        gdf = self._make_geodataframe(tweets, shape_type='centroid')
        gdf.to_file(filename=filename, driver='ESRI Shapefile', crs_wkt=projection_str)
        
        return gdf
    
    def _get_exeptional_days(self):
        df = copy.deepcopy(self.df)
        df['Timestamp'] = self.df['Timestamp'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.000Z").date())
        
        
        avg_tweets_per_day = len(self.tweets) / df['Timestamp'].nunique()
        
        counts = df.groupby(['Timestamp']).agg(len)
        days = counts[counts['Text'] > avg_tweets_per_day]['Text']
        days = list(days.index)
        return days
    
    def _get_interaction_tweet(self, tweet_id, _type):
        tweet_id = str(tweet_id)
        interactions = []
        for tweet in self.tweets:
            if 'referenced_tweets' in tweet:
                for referenced_tweet in tweet['referenced_tweets']:
                    if referenced_tweet['id'] == tweet_id:
                        if referenced_tweet['type'] == _type:
                            interactions.append(tweet)
                        
        print(f'Found {len(interactions)} interactions to the given tweet')
        return interactions
        
    def _tweets_from_account(self, account_key):
        index_list = []
        
        for i, tweet in enumerate(self.tweets):
            if tweet['author_id']['id'] == account_key:
                index_list.append(i)
                
        return index_list
    
    def _has_bot_symbols(self, text):
        symbols = ['%', '°C', 'hpa', 'km/h', ' mm ', 'hPa', 'Km/h', 'kmh', 'm/s']
        text_value = 0
        for symbol in symbols:
            if symbol in text:
                text_value += 1
        
        if '|' in text:
            i = text.find('|')
            if '|' in text[(i+3%len(text)):]: # If multiple ||| are next to each other
                text_value += 2
                
        if text_value >= 2:
            return True
        else:
            return False
    
    def _make_geodataframe(self, tweets, shape_type='centroid'):
        valid_types = ['centroid', 'bbox', 'coordinates']
        if shape_type not in valid_types:
            raise KeyError('Shape type "{shape_type}" is not valid!')
            
        original_length = len(tweets)
        
        tweets = self._append_geometry(tweets, shape_type)
        df_tweets = self._make_dataframe(tweets, shape_type)
        
        df_tweets = df_tweets.dropna(how="any")
        
        print(f'Shapefile does contain {len(df_tweets)} of originally {original_length} tweets!')
        print(f'Location-rate of Tweets: {int(len(df_tweets)/original_length*100)} %')
        
        if shape_type == 'centroid' or shape_type=='coordinates':
            tweets_gdf = gpd.GeoDataFrame(df_tweets, 
                                          geometry = gpd.points_from_xy(df_tweets['Lon'], df_tweets['Lat']))
         
        if shape_type == 'bbox':
            series = gpd.GeoSeries(df_tweets['Bounding_Box'])
            df_tweets = df_tweets.drop(columns=['Bounding_Box'])
            tweets_gdf = gpd.GeoDataFrame(df_tweets, geometry=series)
            
        return tweets_gdf
    
    def _make_dataframe(self, tweets, shape_type):
        
        df = pd.DataFrame(data=np.array([tweet['id'] for tweet in tweets]), columns=['ID'])
        df['Id_User'] = np.array([tweet['author_id']['id'] for tweet in tweets])
        df['Timestamp'] = np.array([tweet['created_at'] for tweet in tweets])
    
        df['Geo'] = np.array([tweet['geo'] if 'geo' in tweet else None for tweet in tweets])
        df['Place_Type'] = np.array([tweet['geo']['place_type'] if 'geo' in tweet and 'place_type' in tweet['geo'] else None for tweet in tweets])
        df['Id_Place'] = np.array([tweet['geo']['id'] if 'geo' in tweet and 'id' in tweet['geo'] else None for tweet in tweets])
        if shape_type == 'bbox':
            df['Place_Name'] = np.array([html.unescape(tweet['geo']['name']) if 'geo' in tweet and 'geo' in tweet['geo'] and 'name' in tweet['geo'] else 'No_Name' for tweet in tweets])
        
        
        if shape_type == 'coordinates' or shape_type == 'centroid':
            df['Lat'] = np.array([tweet['Lat'] if 'Lat' in tweet else None for tweet in tweets])
            df['Lon'] = np.array([tweet['Lon'] if 'Lon' in tweet else None for tweet in tweets])
            
        if shape_type == 'bbox':
            df['Bounding_Box'] = np.array([tweet['Bounding_Box'] if 'Bounding_Box' in tweet else None for tweet in tweets])
        
        print('Created Dataframe sucessfully')
        return df
        
    def _append_geometry(self, tweets, shape_type):
        for i, tweet in enumerate(tweets):
            if 'geo' in tweet:
                if 'geo' in tweet['geo']:
                    if 'bbox' in tweet['geo']['geo']:
                        bbox = tweet['geo']['geo']['bbox']
                        
                        polygon = self._get_polygon(bbox)
                        
                        if shape_type == 'centroid':
                            centroid = self._get_centroid(polygon)
                            tweets[i]['Lon'] = centroid[0]
                            tweets[i]['Lat'] = centroid[1]
                            
                        if shape_type == 'bbox':
                            tweets[i]['Bounding_Box'] = Polygon(polygon)
                            
                        tweets[i]['Name'] = str(tweet['geo']['full_name'].encode('latin-1', 'replace'))[2:-1]
                
                else:
                    if 'coordinates' in tweet['geo'] and shape_type=='coordinates':
                        point = tweet['geo']['coordinates']['coordinates']
                        tweets[i]['Lon'] = point[0]
                        tweets[i]['Lat'] = point[1]
                        
                    elif 'coordinates' in tweet['geo']:
                        continue
                    
                    else:
                        print(f'No geo-specification found at {i}')
        
        
        return tweets                        
                        
    def _get_polygon(self, bbox):
        polygon = ((bbox[0], bbox[1]), (bbox[2], bbox[1]), (bbox[2], bbox[3]), (bbox[0], bbox[3]))
        return polygon
    
    def _get_centroid(self, polygon):
        x_list = [vertex [0] for vertex in polygon]
        y_list = [vertex [1] for vertex in polygon]
        
        _len = len(polygon)
        x = sum(x_list) / _len
        y = sum(y_list) / _len
        return(x, y)


class TweetPlotting(TweetAnalyzer):
    def __init__(self, tweets):  
        
        self.number_of_tweets = len(tweets)
        self.tweets = tweets
        self.df = self.tweets_to_dataframe()
        self.df['Timestamp'] = self.df['Timestamp'].apply(lambda x: x.date())
        self.df = self.df.sort_values(by=['Timestamp'])
        self.df['IsoWeek'] = self.df['Timestamp'].apply(lambda x: str(x.year)[-2:] + ': ' + str(x.isocalendar()[1]))
    
        self._make_statistics()
        
    def plot_tweets_per_week(self, avg_line=True):
        counts = self.df.groupby(['IsoWeek']).agg(len)
        weeks = self.df['IsoWeek'].unique()
        
        fig, axs = plt.subplots()
        
        axs.plot(weeks, counts)
        if avg_line:
            axs.plot(self.df['IsoWeek'], np.full(self.number_of_tweets, self.avg_tweets_per_week), 'r--')
        
        plt.title('Number of Tweets per Week:')        
        plt.ylabel('Number of Tweets')
        plt.xlabel('Week')
        
    
    def plot_tweets_per_day(self, avg_line=True):
        """
        Groups self.df by 'Timestamp' and counts the number of DataFrame-entries every day

        Parameters
        ----------
        avg_line : do you want to show a line with the avg-number of tweets in the analysed timeframe

        """
        idx = pd.date_range(min(self.df['Timestamp']), max(self.df['Timestamp']))
        counts = self.df.groupby(['Timestamp']).agg(len)
        counts.index = pd.DatetimeIndex(counts.index)
        counts = counts.reindex(idx, fill_value=0)
        
        dates = self.df['Timestamp'].unique()
        
        fig, axs = plt.subplots()
        
        axs.plot(idx, counts)
        if avg_line:
            axs.plot(self.df['Timestamp'], np.full(self.number_of_tweets, self.avg_tweets_per_day), 'r--')
        
        plt.title('Number of Tweets per day:')        
        plt.ylabel('Number of Tweets')
        plt.xlabel('Date')
        
    
    def _make_statistics(self):
        self.avg_tweets_per_day = self.number_of_tweets / self.df['Timestamp'].nunique()
        self.avg_tweets_per_week = self.number_of_tweets / self.df['IsoWeek'].nunique()