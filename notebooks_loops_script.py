# -*- coding: utf-8 -*-

import os
import copy
import skmob
import skmob.measures.individual as sk_id
from skmob.measures.collective import random_location_entropy
import datetime
from datetime import datetime as dt
import community
from community import community_louvain
import statistics
import numpy as np
import pandas as pd
import networkx as nx
import geopandas as gpd

csv_dict = {
    'on_offset': [
        [20200406,20200606,"onoffset"],
        [20210406,20210606,"onoffset"],
        [20220406,20220606,"onoffset"]],
    'total': [
        [20200406,20220731,"total"]]
    }
    

def load_subset_multiple(starts, ends, del_one_tweeters=False, timestamp=False, tweets_path='data/tweets/preprocessed_tweets_with_poi_location.csv', samp_size=False):
    """
    This function loads all tweets and segments them into all rolling windows.

    Parameters
    ----------
    starts : list of strings.
        A list of the start dates of all rolling windows that need to be loaded.
    ends : list of strings.
        The same but for the end dates.
    del_one_tweeters : boolean, optional
        Since users with only a single tweet in a respective rolling window are
        not relevant for any movement based statistics, they can be removed.
        The default is False.
    timestamp : boolean, optional
        Is the whole timestamp needed or does the date suffice. If True the date gets used.
        The default is False.
    tweets_path : strings, optional
        Path to the tweet dataset.
        The default is 'data/tweets/preprocessed_tweets_with_poi_location.csv'.
    samp_size : boolean/int, optional
        Should a steady tweet amount be used for each rolling window? If yes, the
        variable should be replaced by the respective number.
        The default is False.

    Raises
    ------
    TypeError
        If the entries for starts and ends are not lists.

    Returns
    -------
    tweets_dfs : list of DataFrames
        A list of DataFrames of tweets, one for each rolling window.

    """
    if type(starts) != list or type(ends) != list:
        raise TypeError("Please give starts and ends as list in order to user this function!")
        
    tweets_df = pd.read_csv(tweets_path)
        
    if 'withheld' in tweets_df.columns:
        tweets_df = tweets_df.drop(columns=['withheld'])
        
    tweets_df['Timestamp'] = tweets_df['Timestamp'].apply(lambda x: dt.strptime(x, "%Y-%m-%d %H:%M:%S"))
    
    if not timestamp:
        tweets_df.Timestamp = tweets_df.Timestamp.apply(dt.date)
        
    tweets_dfs = []
    
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

def make_days(n=1, s=datetime.date(2020, 4, 6), e=datetime.date(2022, 7, 31), overlap=True):
    """
    This function creates a DataFrame of start and end dates by providing the beginning
    and end of the study period, the size of the rolling and whether it shoud overlap.

    Parameters
    ----------
    n : int, optional
        Number of days for a rolling window. The default is 1.
    s : datetime.date, optional
        The start date of the study period. The default is datetime.date(2020, 4, 6).
    e : datetime.date, optional
        The end date of the study period. The default is datetime.date(2022, 7, 31).
    overlap : boolean, optional
        Should the rolling windows overlap each other, in order to have a value for each day?
        Or should there only be on value for each period of time of [subset_size_days] days.
        The default is True.

    Returns
    -------
    csv : DateFrame
        A DataFrame containing the relevant start and end days for each rolling window.

    """
    if overlap:
        freq = 'D'
    
    starts = pd.date_range(s, e - datetime.timedelta(days=n), freq=freq).tolist()
    ends = pd.date_range(s + datetime.timedelta(days=n), e, freq=freq).tolist()
    
    sstarts = [str(x.date()).replace("-", "") for x in starts]
    sends = [str(x.date()).replace("-", "") for x in ends]
    
    denom = [str(n)+'days'] * len(starts)
    
    csv = pd.DataFrame([sstarts, sends, denom]).T

    return csv

def check_file(path):
    """
    Parameters
    ----------
    path : string
        The path and the file, where the calculated metrics should be written.
        If the file does not exist, it is created with the first column (middle_date)

    Returns
    -------
    None.
    """
    if not os.path.exists(path):
        with open(path, "w") as stats:
            stats.writelines('middle_date,')

def middeling(start, end, allow_even_subsets=False):
    """
    Parameters
    ----------
    start : string
        start date as string with format YYYYMMDD.
    end : string
        the same for end date.
    allow_even_subsets : boolean, optional
        if false, a ValueError gets raised if there is an even number of days
        and no middle can be calculated, if it is allowed, the date get rounded up.
        The default is False.

    Raises
    ------
    ValueError
        if even days are not allowed but given.

    Returns
    -------
    middle : TYPE
        DESCRIPTION.

    """
    s = dt.strptime(start, "%Y%m%d").date()
    e = dt.strptime(end, "%Y%m%d").date()
    delta = e - s
    
    if delta.days % 2 == 0:
        print('The Timedelta is an even number of days, allow even numbers or set an Timeframe with an odd number of days!')
        if allow_even_subsets:
            print('The middle date will be rounded up!')
        else:
            raise ValueError("Terminated Loop, choose, different subsets or allow even ones!")
    
    middle = str(s + (delta / 2)).replace("-", "")
    return middle

def notebook_B_05(tweets_dfs, starts, ends):
    """
    This functions efficiently calculates certain metrics. A more comprehesive
    description can be found in the notebook B_05_landuse_metrics.
    
    Parameters
    ----------
    tweets_dfs : list of dataframes
        A list of all rolling windows as dataframes.
    starts : list of strings
        A list of all start dates of all rolling windows.
    ends : list of strings
        Same but for end dates.

    Returns
    -------
    None.

    """
    def count_points(zone):
        idx = zone.name
        poly = zone.geometry
        clipped = gpd.clip(tweets_df, poly)
        no_of_tweets = len(clipped)
        landuse.loc[idx, 'counts'] = no_of_tweets

    _landuse = gpd.read_file(landuse_path).to_crs(4326)
    _landuse['counts'] = 0

    file = pd.read_csv(f'data/{ref}', index_col='start_date')

    for i, (_tweets_df, start, end) in enumerate(zip(tweets_dfs, starts, ends)):
        tweets_df = copy.deepcopy(_tweets_df)
        
        landuse = copy.deepcopy(_landuse)
        print('Getting:', start, end, "notebook B_05 landuse metrics")
        
        tweets_df['wkt'] = gpd.GeoSeries.from_wkt(tweets_df.wkt)
        tweets_df = gpd.GeoDataFrame(tweets_df, geometry='wkt', crs=4326)

        landuse.apply(count_points, axis=1)
    
        stats = {}
        for _cls in landuse.lu_transl.values.tolist():
            if len(tweets_df) == 0:
                rel = np.nan
            else:
                rel = landuse.loc[landuse.lu_transl == _cls].counts.values[0] / len(tweets_df)
            stats[f'rel_tweets_in_{_cls}'] = rel
        
        start = int(start)
        for name, val in stats.items():
            file.loc[start, name] = val
        
        if i % 50 == 0:
            print('Saving')
            file.to_csv(f'data/{ref}')
    
    print("Saving")
    file.to_csv(f'data/{ref}')

def notebook_B_04(tweets_dfs, starts, ends, allow_even_subsets=False):
    """
    This functions efficiently calculates certain metrics. A more comprehesive
    description can be found in the notebook B_04_mobility_metrics_on_od_matrices.
    
    Parameters
    ----------
    tweets_dfs : list of dataframes
        A list of all rolling windows as dataframes.
    starts : list of strings
        A list of all start dates of all rolling windows.
    ends : list of strings
        Same but for end dates.
    allow_even_subsets : boolean, optional
        Value is given to the middeling function, that is defined above.
        The default value is False.

    Returns
    -------
    None.

    """
    def number_of_meaningful_connections(outflow=False):
        numbers_of_meaningful_connections = []
        
        for i in range(mm.shape[0]):
            rel = mm[i, :]
            if outflow:
                rel = mm[:, i]
        
            outgoing_sum = rel.sum()
            
            if outgoing_sum == 0:
                continue
    
            outgoing_rel = rel / outgoing_sum
            count = len(outgoing_rel[outgoing_rel >= 0.05])
    
            numbers_of_meaningful_connections.append(count)
        
        if len(numbers_of_meaningful_connections) == 0:
            return np.nan
        return statistics.mean(numbers_of_meaningful_connections)
        
    def get_mean_distance_to_highest(outflow=False):
        all_distances = []
        for i in range(mm.shape[0]):
            distances = []
    
            rel = mm[i, :]
            if outflow:
                rel = mm[:, i]

            if sum(rel) == 0:
                continue
    
            l = []
            l.extend(list(np.where(rel == max(rel))[0]))
    
            for partner in l:
                partner += 1
                og = barrios_20823.loc[barrios_20823.CODBAIRRO == i+1].geometry.values[0]
                comp = barrios_20823.loc[barrios_20823.CODBAIRRO == partner].geometry.values[0]
                distances.append(og.distance(comp))
    
            all_distances.append(statistics.mean(distances))
    
        if len(all_distances) == 0:
            return np.nan
        
        return statistics.mean(all_distances)
    
    file = pd.read_csv(f'data/{ref}', index_col='start_date')
    
    for i, (_tweets_df, start, end) in enumerate(zip(tweets_dfs, starts, ends)):
        print('Getting:', start, end, "notebook B_04 od matrices stats")
        
        middle = middeling(start, end, allow_even_subsets=allow_even_subsets)
        mm_path = f'./data/movement_matrices/{denom}/mm_{middle}.npy'
        
        if type(sam) == int:
            mm_path = mm_path.split('.npy')[0] + f'_{str(sam)}.npy'
                
        stats = {}
        mm = np.load(mm_path)
        
        for i in range(mm.shape[0]):
            mm[i,i] = 0
        
        stats['no_real_movements'] = mm.sum().sum()

        mm_for_modularity = mm + mm.T # This line is added after the comments of a reviever. Since G = nx.from_numpy_array(mm_for_modularity) later
        # only uses the lower triange in our OD-matrix, we need to add the transposed matrix on top of the original one. This ensures
        # that we have a symmetric matrix to calculate an undirected graph with it and all movements are taken into account.
        # We therefore do not differentiate if a user goes from district a to b or b to a. 
            
        G = nx.from_numpy_array(mm_for_modularity)
        partition = community_louvain.best_partition(G, weight='weight')
        
        try:
            stats['graph_modularity'] = community.modularity(partition, G)
        except:
            stats['graph_modularity'] = np.nan
            
        inflow = {}
        outflow = {}
        for i in range(mm.shape[0]): # y/rows
            outflow['outflow_' + str(i+1) + '_barrio'] = mm[i, :].sum()
        
        for i in range(mm.shape[1]): # x/cols
            inflow['inflow_' + str(i+1) + '_barrio'] = mm[:, i].sum()
        
        stats.update(inflow)
        stats.update(outflow)
        
        stats['number_meaningful_incoming'] = number_of_meaningful_connections()
        stats['number_meaningful_outgoing'] = number_of_meaningful_connections(outflow=True)
        
        stats['mean_distance_strongest_inflow'] = get_mean_distance_to_highest(outflow=False)
        stats['mean_distance_strongest_outflow'] = get_mean_distance_to_highest(outflow=True)
        
        start = int(start)
        for name, val in stats.items():
            file.loc[start, name] = val
        
        if i % 50 == 0:
            print('Saving')
            file.to_csv(f'data/{ref}')
    
    print("Saving")
    file.to_csv(f'data/{ref}')

def notebook_B_03(tweets_dfs, starts, ends):
    """
    This functions efficiently calculates certain metrics. A more comprehesive
    description can be found in the notebook B_03_mobility_metrics_on_user_dicts.
    
    Parameters
    ----------
    tweets_dfs : list of dataframes
        a list of all rolling windows as dataframes.
    starts : list of strings
        a list of all start dates of all rolling windows.
    ends : list of strings
        same but for end dates.

    Returns
    -------
    None.

    """
    def mean2(x):
        if len(x) == 0:
            return 0
        else:
            return statistics.mean(x)
        
    file = pd.read_csv(f'data/{ref}', index_col='start_date')
    
    for i, (_tweets_df, start, end) in enumerate(zip(tweets_dfs, starts, ends)):
        tweets_df = copy.deepcopy(_tweets_df)
        print('Getting:', start, end, "notebook B_03 user dict stats")

        tweets_df['wkt'] = gpd.GeoSeries.from_wkt(tweets_df.wkt).set_crs(4326).to_crs(20823)
        tweets_df['lat'] = gpd.GeoSeries(tweets_df['wkt']).y
        tweets_df['lon'] = gpd.GeoSeries(tweets_df['wkt']).x
        
        tweets_df['lat_cod'] = tweets_df.cod.apply(lambda x: barrios_20823.loc[barrios_20823.CODBAIRRO == x].lat.values[0])
        tweets_df['lon_cod'] = tweets_df.cod.apply(lambda x: barrios_20823.loc[barrios_20823.CODBAIRRO == x].lon.values[0])

        stats = {}
        
        tdf = skmob.TrajDataFrame(tweets_df, latitude='lat', longitude='lon', datetime='Timestamp', user_id='User_ID')
        
        ### 0. Number of Trips
        if len(tdf) == 0:
            continue
        
        stats['poi_number_of_total_trips'] = len(tweets_df) - tweets_df.User_ID.nunique()
        
        rg_df = sk_id.radius_of_gyration(tdf, show_progress=False)
        stats["mean_rog"] = rg_df.radius_of_gyration.mean()
        stats['std_rog'] = rg_df.radius_of_gyration.std()
        
        jl = sk_id.jump_lengths(tdf, show_progress=False).jump_lengths
        
        meaned = jl.apply(mean2)
        
        stats["jl_simple_means_over_user_means"] = meaned.mean()
        stats["jl_std_over_user_means"] = meaned.std()
        stats["jl_simple_means_only_with_movement_user_means"] = meaned[meaned > 0].mean()
        stats["jl_std_only_with_movement_user_means"] = meaned[meaned > 0].std()
        
        all_moves = []
        jl.apply(lambda x: all_moves.extend(x))
        all_moves = pd.Series(all_moves)
        
        stats['mean_total_distance'] = all_moves.sum() / stats['poi_number_of_total_trips']
        stats['std_total_distance'] = all_moves.std()
        
        wt = sk_id.waiting_times(tdf, show_progress=False).waiting_times
        stats['avg_avg_time_between_tweets_per_user'] = wt.apply(mean2).mean()
        stats['avg_time_between_tweets_total'] = wt.apply(sum).sum() / stats['poi_number_of_total_trips']
        
        ### 4. Average amount of Trips per User
        stats['avg_number_of_trips_per_user'] = stats['poi_number_of_total_trips'] / tdf.uid.nunique()
        
        ### 5. Maximum distance
        max_dist = sk_id.maximum_distance(tdf, show_progress=False).maximum_distance
        stats['mean_max_distance'] = max_dist.mean()
        stats['std_max_distance'] = max_dist.std()
        
        ### 7. Number of Locations
        locs = sk_id.number_of_locations(tdf, show_progress=False).number_of_locations
        stats['mean_number_of_locations'] = locs.mean()
        stats['std_number_of_locations'] = locs.std()
        
        ### 8. Maximum distance from home
        home_dist = sk_id.max_distance_from_home(tdf, show_progress=False).max_distance_from_home
        stats['mean_max_dist_from_home'] = home_dist.mean()
        stats['std_max_dist_from_home'] = home_dist.std()
        
        ### 9. Mean random location entropy
        #############################################
        tweets_df['lat'] = tweets_df['lat_cod']
        tweets_df['lon'] = tweets_df['lon_cod']
        #############################################
        
        tdf_barr = skmob.TrajDataFrame(tweets_df, latitude='lat', longitude='lon', datetime='Timestamp', user_id='User_ID')
        
        stats['mean_random_location_entropy_barrios'] = random_location_entropy(tdf_barr, show_progress=False).mean()[2]
        
        start = int(start)
        for name, val in stats.items():
            file.loc[start, name] = val
            
        if i % 50 == 0:
            print('Saving')
            file.to_csv(f'data/{ref}')
      
    print("Saving")
    file.to_csv(f'data/{ref}')
      
def notebook_B_02(tweets_dfs, starts, ends, allow_even_subsets=False):
    """
    This functions efficiently calculates OD-matrices. A more comprehesive
    description can be found in the notebook B_02_OD_matrices.
    
    Parameters
    ----------
    tweets_dfs : list of dataframes
        a list of all rolling windows as dataframes.
    starts : list of strings
        a list of all start dates of all rolling windows.
    ends : list of strings
        same but for end dates.
    allow_even_subsets : boolean, optional
        value is given to the middeling function, that is defined above.
        The default value is False.

    Returns
    -------
    None.

    """
    def create_matrix(df, diag_to_0=False):   
        df = df.reset_index(drop=True)
        user_ids = df.User_ID.unique().tolist()

        user_sequence = {_id: [] for _id in user_ids}

        for i in range(len(df)):
            user_sequence[df.User_ID[i]].append(df.cod[i])

        matrix = np.zeros((164, 164))
        for key in user_sequence:
            _len = len(user_sequence[key])
            if _len > 0:
                for i in range(0, _len-1):
                    matrix[user_sequence[key][i], user_sequence[key][i+1]] = \
                        matrix.item((user_sequence[key][i], user_sequence[key][i+1])) + 1

        # set diagonal to 0
        def fun_diag_to_0(matrix):
            for i in range(0, 164):
                matrix[i, i] = 0
            return matrix
        
        if diag_to_0:
            matrix = fun_diag_to_0(matrix)

        full_movement_matrix = pd.DataFrame(matrix)
        full_movement_matrix = full_movement_matrix.drop([0]).drop([0], axis=1)
        
        return full_movement_matrix, user_sequence

    for tweets_df, start, end in zip(tweets_dfs, starts, ends):
        middle = middeling(start, end, allow_even_subsets=allow_even_subsets)
        
        print("Getting:", start, end, "notebook_B_02 OD Matrices")
        mm_path = f'./data/movement_matrices/{denom}/mm_{middle}.npy'
        if type(sam) == int:
            mm_path = mm_path.split('.npy')[0] + f'_{str(sam)}.npy'
        
        full_movement_matrix, user_sequence = create_matrix(tweets_df, diag_to_0=True)
        
        path = mm_path.split("/mm")[0]
        if not os.path.exists(mm_path.split("/mm")[0]):
            os.mkdir(path)
        
        np.save(mm_path, full_movement_matrix.to_numpy())
        
def notebook_B_01(tweets_dfs, starts, ends, allow_even_subsets=False):
    """
    This functions efficiently calculates certain metrics. A more comprehesive
    description can be found in the notebook B_05_landuse_metrics.
    
    Parameters
    ----------
    tweets_dfs : list of dataframes
        a list of all rolling windows as dataframes.
    starts : list of strings
        a list of all start dates of all rolling windows.
    ends : list of strings
        same but for end dates.
    allow_even_subsets : boolean, optional
        value is given to the middeling function, that is defined above.
        The default value is False.
    
    Returns
    -------
    None.
    
    """
    check_file(f"data/{ref}") 
    file = pd.read_csv(f'data/{ref}', index_col='middle_date')
    for i, (tweets_df, start, end) in enumerate(zip(tweets_dfs, starts, ends)):
        
        print("Getting:", start, end, "notebook B_01")
    
        stats = {}
        
        middle = middeling(start, end, allow_even_subsets=allow_even_subsets)

        stats['start_date'] = start
        stats['end_date'] = end
        stats["no_of_tweets"] = len(tweets_df)
        stats["number_unique_users"] = tweets_df.User_ID.nunique()
        stats["median_tweets_per_user"] = tweets_df.groupby('User_ID').Tweet_ID.count().median()
        try:
            stats["mean_tweets_per_user"] = stats["no_of_tweets"] / stats["number_unique_users"]
        except:
            stats["mean_tweets_per_user"] = np.nan
        
        counted = tweets_df.groupby('User_ID').count()
        stats["n_user_more_than_one_tweet"] = len(counted[counted['Timestamp'] > 1])
        stats["n_users_with_more_than_one_location_point"] = sum(tweets_df.groupby('User_ID').nunique()['wkt'] > 1)
        stats["n_users_with_more_than_one_cod"] = sum(tweets_df.groupby('User_ID').nunique()['cod'] > 1)
        
        middle = int(middle)
        for name, val in stats.items():
            file.loc[middle, name] = val
            
        if i % 50 == 0:
            print("Saving")
            file.to_csv(f'data/{ref}')

    print("Saving")
    file.to_csv(f'data/{ref}')
        
def run_all_notebooks(subset_size_days, tweets_path, sample=False, del_one_tweeters=True, overlap=True, subset_csv=False, allow_even_subsets=False):
    """
    This function creates the setup for all metrics of one rolling window size to be calculated in one setting.
    E.g. the metrics of the whole time period should be calculated using overlapping 5-day rolling windows:
        First, global variables are defined. These handle saving and loading processes and to distinguish between metrics based on all tweets or with
        a steady sample size of tweets.
        
        If no manual csv-file is provided, a list of all rolling windows is created by using the make_days function.
        This defines the start and end dates for each rolling window, so one line for each consecutive 5 days in the whole time period
        (20200406 - 20200411; 20200407 - 20200412; ...; 20220726 - 20220721).
        
        Then the load_subset_multiple function is called. This function loads the whole dataset and segments it into a list of dataframes.
        Each dataframe contains all tweets in certain rolling window. So for each of the 5 consecutive day period there is one dataframe.
        
        Finally, each notebook-function is run by giving this list of dataframes and their respective start and end dates (and sometimes
        the allow_even_subsets variable). So for each rolling window, alle metrics are calculated. The metrics are always saved from the notebook-
        functions using the global ref variable.

    Parameters
    ----------
    subset_size_days : int
        The size of the rolling windows.
    tweets_path : string
        Path to the source tweets.
    sample : boolean/int, optional
        Should a steady tweet amount be used for each rolling window? If yes, the
        variable should be replaced by the respective number.
        The default is False.
    del_one_tweeters : boolean, optional
        Since users with only a single tweet in a respective rolling window are
        not relevant for any movement based statistics, they can be removed.
        The default is True.
    overlap : boolean, optional
        Should the rolling windows overlap each other, in order to have a value for each day?
        Or should there only be on value for each period of time of [subset_size_days] days.
        The default is True.
    subset_csv : boolean/string, optional
        A path to a csv file containing certain subsets. Our study contains onset/offset periods. These were
        defined manually by providing this csv file. In this case the False has to be replaced with a path.
        
        If a string is given here,, subeset_size_days is effectively useless, since the rolling windows are replaced by the ones given
        in the csv path here.
        
        The default is False.
    allow_even_subsets : boolean, optional
        This parameter is relevant for some of the notebook-functions above. It is needed to calculate the middle
        date for rolling windows.
        The default is False.

    Raises
    ------
    ValueError
        The Error is raised when, even rolling windows are not allowed, but subset_size_days is an even number.

    Returns
    -------
    None.
    """
    if subset_size_days % 2 == 0:
        print('The Timedelta is an even number of days, allow even numbers or set an Timeframe with an odd number of days!')
        if allow_even_subsets:
            print('The middle date will be rounded up!')
        else:
            raise ValueError("Terminated Loop, choose, different subsets or allow even ones!")
    
    global ref
    global sam
    global denom
    denom = str(subset_size_days) + "days" + "_overlap" if overlap else str(subset_size_days) + "days"
    sam = sample
    
    if type(subset_csv) == str:
        csv = pd.DataFrame(csv_dict[subset_csv])
        #csv = pd.read_csv(subset_csv).drop(columns='Unnamed: 0')
        denom = subset_csv
        print(denom)
        print(csv)
    else:
        csv = make_days(subset_size_days, s=study_period_start, e=study_period_end)
        
    csv.columns = ["start", "end", "denom"]
    
    ref = "statistics/statistics_" + denom + ".csv"
    if type(sam) == int:
        ref = "sample_based_statistics/statistics_" + denom + ".csv"
        ref = ref.split('.csv')[0] + "_" + str(sam) + ".csv"

    starts = csv["start"].astype(str).tolist()
    ends = csv["end"].astype(str).tolist()

    tweets_dfs = load_subset_multiple(starts, ends, del_one_tweeters=del_one_tweeters, \
                                           samp_size=sam, tweets_path=tweets_path) #, tweets_path='data/tweets/test_sample.csv')


    notebook_B_01(tweets_dfs, starts, ends, allow_even_subsets=allow_even_subsets)
    notebook_B_02(tweets_dfs, starts, ends, allow_even_subsets=allow_even_subsets)
    notebook_B_03(tweets_dfs, starts, ends) 
    notebook_B_04(tweets_dfs, starts, ends, allow_even_subsets=allow_even_subsets)
    notebook_B_05(tweets_dfs, starts, ends)

def main():
    """
    Here it starts: First the neighborhoods geometries are loaded and projected ot EPSG:20823
    as well as the landuse classes. The paths to the tweet data are defined.
    
    Then, the run_all_notebooks-function is called for all rolling windows or other timeframes needed.

    Returns
    -------
    None.

    """
    global study_period_start
    global study_period_end
    study_period_start = datetime.date(2020, 4, 6)
    study_period_end = datetime.date(2020, 7, 1)
    
    barrios_path = 'data/shps/neighborhoods.shp'
    
    global landuse_path
    landuse_path = 'data/shps/land_use_land_cover.shp'

    global barrios_20823
    barrios_20823 = gpd.read_file(barrios_path).to_crs(20823)
    barrios_20823.CODBAIRRO = barrios_20823.CODBAIRRO.astype(int)
    barrios_20823['geometry'] = barrios_20823.geometry.representative_point()# oder representative_point?
    barrios_20823['lat'] = barrios_20823['geometry'].y
    barrios_20823['lon'] = barrios_20823['geometry'].x
    
    full_dataset = 'data/tweets/preprocessed_tweets_with_poi_location.csv'
    
    # get all metrics for all uneven rolling window sizes (1 to 31)
    for i in range(1, 32, 2):
        run_all_notebooks(i, tweets_path=full_dataset)

    # get the metrics for the whole timeframe (total.csv) and for the three onset/offset periods (on_offset.csv)
    # by setting a subset_csv string, the 1 for as first argument is irrelevant and just a placeholder
    run_all_notebooks(1, tweets_path=full_dataset, subset_csv='total', allow_even_subsets=True)
    run_all_notebooks(1, tweets_path=full_dataset, subset_csv='on_offset', allow_even_subsets=True)
   
    # # get metrics for this 3 chosen rolling window sizes by using a steady sample size
    run_all_notebooks(1, tweets_path=full_dataset, sample=61)
    run_all_notebooks(7, tweets_path=full_dataset, sample=1115)
    run_all_notebooks(15, tweets_path=full_dataset, sample=2931)
    
    print("FINISHED!")
    
if __name__ == "__main__":
    main()