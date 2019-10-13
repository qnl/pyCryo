#!/usr/bin/env python

""" differents tools to reads bluefors log file """

import pandas as pd
import datetime

channel_labels = {1:'50K', 2:'4K', 5:'still', 6:'MXC'}


def load_BF_log_single_day(path_to_log, day, channels=[1, 2, 5, 6]):
    """ Construct a single dataframe out of the log file from BlueFors """
    dfs = []
    labels = [channel_labels[k] for k in channels]
    # change day into a datetime format for easier manipulation
    if isinstance(day, str):
        day = datetime.date.fromisoformat(day)
    log_files = [path_to_log + '{1}/CH{0} T {1}.log'.format(ch, day.strftime('%Y-%m-%d')[2:]) for ch in channels]
    for fname, label in zip(log_files, labels):
        print(label)
        df = pd.read_csv(fname,
                         sep=",",
                         header=None)
        df.columns = ['date', 'time', label]
        df['datetime'] = pd.to_datetime(df['date'] +' '+ df['time'],
                                        format=' %d-%m-%y %H:%M:%S')
        df = df.drop('date', 1)
        df = df.drop('time', 1)
        #del df['date']
        #del df['time']
        dfs.append(df)
    
    # merging all the dframes into one
    from functools import reduce
    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['datetime'], how='outer'), dfs) 
    return df_merged

def load_BF_log(path_to_log, day, n_day=1, channels=[1,2,5,6]):
    """ load and concatenate several day fo BF logfiles"""
    if isinstance(day, str):
        day = datetime.date.fromisoformat(day)
    df = []
    for k in range(n_day):
        df.append(load_BF_log_single_day('log/', day=day + datetime.timedelta(days=k)))
    return pd.concat(df)