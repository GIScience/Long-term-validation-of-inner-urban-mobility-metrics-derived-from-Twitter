# -*- coding: utf-8 -*-
"""
Created on Wed Mar 10 11:00:54 2021

@author: Simon Groß

This code provides the possibility to get Twitter data through the V2 API.

"""

################ Other Modules ################
import os
import copy
import html
import time
import pickle
import requests
import pandas as pd

BEARER_TOKEN = os.environ.get("BEARER_TOKEN")
CONSUMER_KEY = 1
CONSUMER_SECRET = 1

access_token = ''
access_token_secret = ''

backup_path = "D:"
              
class FullArchiveV2():
    def __init__(self, default_query='from:twitter',
                 active_fields='created_at,text,public_metrics,referenced_tweets,geo,lang,conversation_id',
                 active_expansions='author_id,geo.place_id',
                 place_fields='full_name,geo,id,name,place_type'):
        """
        Initialise your Object to search the full Twitter Archive based on the API V2.
        You will need a global variable 'creds' as a dict containing the key 'Bearer_Token'
        
        Parameters
        ----------
        default_query : string, optional
            You can specify the query at initialisation. The default is 'from:dwd_presse'.
        active_fields : string, optional
            The fields, you want in your Twitter response. The default is 'created_at,text,public_metrics,referenced_tweets,geo,lang'.
        active_expansions : string, optional
            the expansions you want to enhance your response. The default is 'author_id,geo.place_id'.
        place_fields : string, optional
            specifiy, which parameters of the place-object you want in your response. The default is 'full_name,geo,id,name,place_type'.

        Many more fields and expansions can be added!
        More information on: https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
        """
        
        self.search_url = "https://api.twitter.com/2/tweets/search/all"
        self.bearer_token = BEARER_TOKEN
        
        self.query_params = {}
        self.active_fields = active_fields
        self.active_expansions = active_expansions
        
        # if 'author_id' in self.active_expansions:
        #     self.user_fields = user_fields
        #     self.query_params['user.fields'] = self.user_fields
        
        if 'geo.place_id' in self.active_expansions:
            self.place_fields = place_fields
            self.query_params['place.fields'] = self.place_fields
            
        self.query_params['query'] = default_query
        self.query_params['tweet.fields'] = self.active_fields
        self.query_params['expansions'] = self.active_expansions

    def set_query_manual(self, query):
        """
        If you want to set the query manually:
            AND: ' ' (space) betweet the parameters
            OR: ' OR ' between the parameters
            NOT: '-' befor the parameter
            GROUPING: '()' enclosing the group

        Parameters
        ----------
        query : string
            your query. E.g. '(#sabine OR #sturm OR #gewitter) from:dwd_presse @person -is:retweet'
            -> Tweets from the account @dwd_presse, containing one of the three hashtags, 
            mentioning the account @person, excluding retweets.
            
            Full list of operators:
            https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#list
            
        Raises
        ------
        ValueError
            In case the query is too long
        """
        if len(query) > 1024:
            raise ValueError(f'The query is more than 1024 characters long. Length: {len(query)}')
        
        self.query_params['query'] = query
        print('Query set to:', query)
        
    def set_parameters(self, hashtags=None, phrases=None, start_time=None, 
                    end_time=None, max_results_per_page=10, from_accounts=None,
                    to_accounts=None, mentions=None, get_retweets=None, get_replies=None,
                    get_quotes=None, lang=None, has_media=None, has_links=None, has_geo=None,
                    point_and_radius=None, bbox=None, place_id=None):
        """
        Between multipe list-objects there will always be OR-logic, between different parameters
        always AND logic, it can also be a single string.
        
        In order to negate phrases, bounding-boxes, or other stackable parameters use a "--" in
        front of it:
            e.g. place="--xxxxxxxxxxxxxx"

        Parameters
        ----------
        hashtags : list of strings or string, optional
            Will be initialised with OR logic!
            If AND logic is required describe the hashtags in a singular
            list parameter with a space in between: ['#sabine #xavier', '#friederike']
            --> ((#sabine AND #xavier) OR #friederike)
            excuding works with a '-' beforehand --> -#sabine.
            The default is None.
        phrases : list of strings or string, optional
            certaint phrases to be contained in the tweet.
            Same logic as hashtags.
            The default is None.
        from_accounts : list of strings or string, optional
            account list of sending accounts.
            Same logic as hashtags.
            The default is None.
        to_accounts : list of strings or string, optional
            replies to a given user. 
            Same logic as hashtags.
            The default is None.
        mentions: list of strings or string, optional
            tweets which mentions these accounts.
            Same logic as hashtags.
            The default is None.
        start_time : string: ISO Timestamp YYYY-MM-DDTHH:MM:SSZ, optional
            from when are the Tweets requested. The default is None.
        end_time : string: ISO Timestamp YYYY-MM-DDTHH:MM:SSZ, optional
            until when. The default is None.
        max_results_per_page : int, optional
            maximum of results per query. The default is 10, maximum is 500.
        get_retweets : bool, optional
            do you want retweets?. The default is True.
        get_replies : same
        get_quotes : same
        has_media : bool, optional
            do you want tweets containing pictures, videos, etc.?
        has_links : same with links
        lang : string, optional
            the language of the tweets
        bbox : tupel, optional
            format: (west long, south lat, east long, north_lat)
        place_id : int/string or list of strings
            the name or the id of the place the tweets was from. Use a Tupel with (['place_id', 'place_id'], False)
            in order to use AND logic inbetween

        Returns
        -------
        None.

        """
        self.next_token = None
        
        self.hashtags = self._check_multiple(hashtags)
        self.phrases = self._check_multiple(phrases)
        self.from_accounts = self._check_multiple(from_accounts)
        self.to_accounts = self._check_multiple(to_accounts)
        self.mentions = self._check_multiple(mentions)
        if place_id is not None:
            self.place = (self._check_multiple(place_id[0]), place_id[1]) # gerade etwas unschön gelöst....
        else:
            self.place = None
        
        self.get_retweets = get_retweets
        self.get_replies = get_replies
        self.get_quotes = get_quotes
        self.has_media = has_media
        self.has_links = has_links
        self.has_geo = has_geo
        
        
        self.point_and_radius = self._check_multiple(point_and_radius)
        self.point_and_radius = self._check_radius()
        
        if self.point_and_radius is not None:
            self.point_and_radius = [str(point[1]) + ' ' + str(point[0]) + ' ' + str(point[2]) + point[3] for point in self.point_and_radius]
        
        if bbox is not None:
            self.bbox = self._check_multiple(bbox)
            self.bbox = [str(box[0]) + ' ' + str(box[1]) + ' ' + str(box[2]) + ' ' + str(box[3]) for box in self.bbox]
        else:
            self.bbox = None
        
        self.start_time = start_time
        self.end_time = end_time
        if max_results_per_page <= 500 and max_results_per_page >= 10:
            self.max_results_per_page = max_results_per_page
        else:
            raise ValueError('Results per page must be between 10 and 500')
            
        self.language = lang
        
        self._build_query_params()
        
        self.full_response = {'data': [],
                              'errors': [],
                              'includes': {'users': [],
                                           'places': []}
                              }
        
        print('Your Query:\n')
        for key in self.query_params:    
            print(f'{key}: {self.query_params[key]}\n')
        
        print(f'=================================\nYour Query is {len(self.query_params["query"])} characters long')
    
    def get_tweets(self, number_of_pages=1, save_inbetween=False):
        """
        Sends requests to Twitter containing the specified parameters.
        Maximum of 1 request per second and 300 requests per 15 minutes is enforced.
        Pagination occures automatically.
        The self.all_tweets variable will be created or overwritten.
        Have Fun!
        
        Parameters
        ----------
        number_of_pages : int
            Number of requests you want to send to Twitter. The amount of tweets per
            request is specified in the variable max_results_per_page. The default is 1.

        Returns
        -------
        list
            A list of all tweets.

        """
        self.number_of_pages = number_of_pages
        self.all_tweets = []
        return self._get_tweets(save_inbetween=save_inbetween)
        
        for tweet in self.all_tweets:
            tweet['text'] = html.unescape(tweet['text'])
        
        return self.all_tweets
    
    def get_places(self):
        if self.full_response is None:
            raise ValueError('No tweets pulled yet, response is empty!')
            
        if 'includes' not in self.full_response:
            raise ValueError('No includes in response!')
            
        if 'places' not in self.full_response['includes']:
            
            raise ValueError('No places in tweet data!')
        
        places = self.full_response['includes']['places']
        
        return places
        
    def wip_place_statistics(self):
        """
        Get the attached places of all tweets in the full json response and the number of their occurences

        Raises
        ------
        ValueError
            if no places are stored in the includes attachement of the full response.

        Returns
        -------
        places : TYPE
            DESCRIPTION.

        """
        if self.full_response is None:
            raise ValueError('No places pulled yet!')
        
        places = copy.deepcopy(self.full_response['includes']['places'])
        
        for i, place in enumerate(self.full_response['includes']['places']):
            place_counter = 0
            _id = place['id']
            for tweet in self.full_response['data']:
                if 'id' in tweet['geo']:
                    place_id = tweet['geo']['id']
                    if place_id == _id:
                        place_counter += 1
                else:
                    print('No place id!')
            places[i]['Occurences'] = place_counter
            
        return places
           
    def get_more_tweets(self, number_of_pages=1, save_inbetween=False):
        """
        Gets more pages from the last query, in case you want more pages, but don't want to download all tweets again

        Parameters
        ----------
        number_of_pages : int, optional
            Number of additional requests. The default is 1.

        Raises
        ------
        ValueError
            In case there was no more page from your last query.

        Returns
        -------
        list
            A list of all tweets (also the ones from the previous query.

        """
        if self.next_token == None:
            raise ValueError('No next_token from last query!')
            
        self.number_of_pages = number_of_pages
        self._get_tweets(self.next_token, save_inbetween=save_inbetween)
        
        for tweet in self.all_tweets:
            tweet['text'] = html.unescape(tweet['text'])
        
        return self.all_tweets
    
    def _get_tweets(self, next_token=None, request_counter=0, save_inbetween=False):
        request_counter += 1
        print(f'Getting Page {request_counter}...')
        
        
        if next_token is not None:
            self.query_params['next_token'] = next_token
        
        json_response = self._get_tweets_single_page()
        return json_response
        
        if json_response['meta']['result_count'] == 0 and request_counter == 1:
            print('No results found!\n')
            return
        
        tweets_current_page = []
        
        if 'data' not in json_response:
            print('#######################\nNo Tweets on this page!\n#######################\n')
            print(f'Responsible Next-Token: {next_token}')
            print(json_response)
            json_response['data'] = []
            
        else:    
            tweets_current_page = json_response['data']
        
        #### Attaching the included Objects to the tweets ####
        if 'includes' in json_response:
            user_current_page = json_response['includes']['users']
            
            for i, tweet in enumerate(tweets_current_page):
                user_id = tweet['author_id']
                
                for user in user_current_page:
                    if user_id == user['id']:
                        tweets_current_page[i]['author_id'] = user
            
            if 'places' in json_response['includes']:
                places_current_page = json_response['includes']['places']
                for i, tweet in enumerate(tweets_current_page):
                    if 'geo' in tweet:
                        if 'place_id' in tweet['geo']:
                            place_id = tweet['geo']['place_id']
                            
                            for place in places_current_page:
                                if place_id == place['id']:
                                    tweets_current_page[i]['geo'] = place
                            
                print(f'Attached {len(places_current_page)} Places')
            
            
        
        else:
            json_response['includes'] = {'users': [], 'places': []}
        
        self.all_tweets.extend(tweets_current_page)
        
        print(f"Last Date: {json_response['data'][-1]['created_at'][:10]}")
                
        self.full_response['data'].extend(json_response['data'])

        places = [place for place in json_response['includes']['places'] if place not in self.full_response['includes']['places']]
        self.full_response['includes']['places'].extend(places)
        
        users = [user for user in json_response['includes']['users'] if user not in self.full_response['includes']['users']]
        self.full_response['includes']['users'].extend(users)
        print('Got tweets successfully!\n=================')
        
        if save_inbetween and request_counter % 50 == 0:
            with open(os.path.join(backup_path, 'backup.txt'), "wb") as fp:
                pickle.dump(self.all_tweets, fp)
            with open(os.path.join(backup_path, 'backup_full_response.txt'), "wb") as fp:
                pickle.dump(self.full_response, fp)
        
        ##### Start Recursion #####
        
        if 'next_token' in json_response['meta']:
            next_token = json_response['meta']['next_token']
            self.next_token = next_token
            
            if request_counter == self.number_of_pages:
                print(f'Got {self.number_of_pages} pages containing {len(self.all_tweets)} tweets!')
                
                if self.number_of_pages != 1:
                    del self.query_params['next_token']
            
                return
            
            time.sleep(1) # One request per second!
            self._check_time(request_counter)
            self._get_tweets(next_token=next_token, request_counter=request_counter, save_inbetween=save_inbetween)
            
        else:
            print('No next Token')
            del self.query_params['next_token']
            print(f'No more Pages!\nGot {len(self.all_tweets)} Tweets\n')
            self.next_token = None
            
    def _get_tweets_single_page(self):
        headers = self._create_headers(self.bearer_token)
        json_response = self._connect_to_endpoint(headers)
        return json_response
    
    def _build_query_params(self):
        """
        Build the query from the specified values
        -------
        the query dictionary gets built, so it's usable for get_tweets()
        """
        query = ''
        
        if self.hashtags is not None:
            query += self._build_subquery(self.hashtags)
            
        if self.phrases is not None:
            query += self._build_subquery(self.phrases, before='"', after='"')
            
        if self.from_accounts is not None:
            query += self._build_subquery(self.from_accounts, before='from:')
            
        if self.to_accounts is not None:
            query += self._build_subquery(self.from_accounts, before='to:')
            
        if self.mentions is not None:
            query += self._build_subquery(self.from_accounts, before='@')
            
        if self.point_and_radius is not None:
            query += self._build_subquery(self.point_and_radius, before='point_radius:[', after=']')
            
        if  self.bbox is not None:
            query += self._build_subquery(self.bbox, before='bounding_box:[', after=']')
        
        if self.place is not None:
            query += self._build_subquery(self.place[0], before='place:', or_logic=self.place[1])
        
        if self.get_retweets is not None:
            query += 'is:retweets ' if self.get_retweets else '-is:retweet '
                  
        if self.get_replies is not None:
            query += 'is:reply ' if self.get_replies else '-is:reply '
                
        if self.get_quotes is not None:
            query += 'is:quote ' if self.get_quotes else '-is:quote '
                
        if self.has_media is not None:
            query += 'has:media ' if self.has_media else '-has:media '
                
        if self.has_links is not None:
            query += 'has:links ' if self.has_links else '-has:links '
            
        if self.has_geo is not None:
            query += 'has:geo ' if self.has_geo else '-has:geo '
                
        if self.language != None:  
            query += 'lang:' + self.language + ' '
            
        
            
            
        if query[-1] == ' ':
            query = query[:-1]
            
        if len(query) > 1024:
            raise ValueError(f'The query is more than 1024 characters long. Length: {len(query)}')

        self.query_params['query'] = query
        self.query_params['max_results'] = self.max_results_per_page
        if self.start_time is not None:
            self.query_params['start_time'] = self.start_time
        if self.start_time is not None:
            self.query_params['end_time'] = self.end_time
    
    def _build_subquery(self, params, before='', after='', or_logic=True):
        logic = ' OR ' if or_logic else ' '
        _len = len(logic)
        
        subquery = ''
        brackets = ['(', ')']
        if len(params) == 1:
            brackets = ['','']
              
        for part in params:
            if part[0:2] != '--':
                subquery += logic + before + part + after
            else:
                subquery += logic + '-' + before + part[2:] + after
                
        subquery = brackets[0] + subquery[_len:] + brackets[1]
        return subquery + ' '
    
    def _create_headers(self, bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers
        
    def _connect_to_endpoint(self, headers):
        response = requests.request("GET", self.search_url, headers=headers, params=self.query_params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()
    
    def _check_time(self, request_counter):
        if request_counter % 300 == 0:
            i = 8
            while i>0:
                print(f'300 Requests! Waiting for {i} Minutes')
                time.sleep(60)
                i -= 1
    
    def _check_multiple(self, param):
        if param is None:
            return param
        if type(param) != list:
            param = [param]
            return param
        return param
    
    def _check_radius(self):
        if self.point_and_radius is None:
            return None
        
        for point in self.point_and_radius:
            if point[3] == "mi" and point[2] > 25:
                raise ValueError('The maximum radius is 25 miles!')
        
            if point[3] == "km" and point[2] > 40:
                raise ValueError('The maximum radius is 40 kilometers!')

def save_tweets_raw(path, tweets):
    """
    Save a list of Tweets without loss of data at the specified path
    """
    with open(path, "wb") as fp:
        pickle.dump(tweets, fp)
    
def load_tweets(path):
    """
    Load a list as saved before with the save_tweets_raw function
    """
    with open(path, "rb") as fp:
        data = pickle.load(fp)

    return data
           
def write_to_csv(texts, filepath):
    df = pd.DataFrame(texts)
    df.to_csv(filepath, sep=';', index=False)
              
if __name__ == '__main__':
    getter = FullArchiveV2()
    
    # example to get tweets in rio, excuding the surrounding cities and rio itself (bounding box to big to work with)
    getter.set_parameters(start_time='2019-05-31T00:00:00.000Z',
                              end_time='2019-05-31T23:59:59.000Z',
                              # bbox=[(-43.7968239565804751, -23.0877912127987628, -43.44485299668037115, -22.7456808960606871),
                              #         (-43.44485299668037115, -23.0877912127987628, -43.0928820367802672, -22.7456808960606871)],
                              has_geo=True,
                              get_retweets=False,
                              hashtags=["trump"],
                              # place_id=(['--97bcdfca1a2dca59', '--41bf05f3b26396e4', '--596a82c8c53236bd', 
                              #           '--4029837e46e8e369', '--1c88433143d383e1', '--59373f0a295160e4', 
                              #           '--7343e9c57b3427b5', '--3b5c5c9c62f7c538',
                              #           '--abbb45debbf38127', '--b1a5f3bbff698d24', '--d1fc0c973adbff22'], False),
                              max_results_per_page=20)