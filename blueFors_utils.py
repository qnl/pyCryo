#!/usr/bin/env python

""" differents tools to reads bluefors log file """

import pandas as pd
import datetime
import matplotlib.pyplot as plt

channel_labels = {1:'50K', 2:'4K', 5:'still', 6:'MXC'}

def load_BF_log_single_day(path_to_log, day, channels=[1, 2, 5, 6]):
    """ Construct a single dataframe out of the log file from BlueFors """
    from functools import reduce
    labels = [channel_labels[k] for k in channels]
    dfs = []
    # change day into a datetime format for easier manipulation
    if isinstance(day, str):
        day = datetime.date.fromisoformat(day)

    # listing all the log files in the day folder
    log_files = [path_to_log + '{1}/CH{0} T {1}.log'.format(ch, day.strftime('%Y-%m-%d')[2:]) for ch in channels]

    for fname, label in zip(log_files, labels):
        df = pd.read_csv(fname,
                         sep=",",
                         header=None)
        # due to Bluefors notation of time, we needs to do a little hack:
        df.columns = ['date', 'time', label]
        df['datetime'] = pd.to_datetime(df['date'] +' '+ df['time'],
                                        format=' %d-%m-%y %H:%M:%S')
        df = df.drop('date', 1)
        df = df.drop('time', 1)
        df.rename(columns={'datetime':'time'}, inplace=True)
        dfs.append(df)
    
    # merging all the dframes into one
    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['time'], how='outer'), dfs) 
    return df_merged


class blueFors_log(object):
    """docstring for blueFors_log"""
    def __init__(self, day, logs_path, offset=None, n_days=1, channels=[1,2,5,6]):
        super(blueFors_log, self).__init__()
        self.logs_path = logs_path

        # change day into a datetime format for easier manipulation
        if isinstance(day, str):
            self.day = datetime.date.fromisoformat(day) 

        self.n_days = n_days

        self.channels = channels
        self.labels = [channel_labels[k] for k in channels]

        # get the data out of the log:
        self.get_df()
        if offset is not None:
            self.offset = datetime.timedelta(hours=offset)

        self.set_offset()

    def get_df(self):
        """ load and concatenate several day fo BF logfiles"""
        df = []
        for k in range(self.n_days):
            df.append(load_BF_log_single_day(self.logs_path,
                                             day=self.day + datetime.timedelta(days=k)))
        self.df = pd.concat(df)

    def set_offset(self, offset=None):
        """ create an offsetted time"""
        if self.offset == None:
            self.df['time_offset'] = (self.df['time']-min(self.df['time']))/datetime.timedelta(hours=1)
        else:
            self.df['time_offset'] = (self.df['time']-min(self.df['time']-self.offset))/datetime.timedelta(hours=1)


    def plot(self, axs=None):
        """ Plot the data"""
        if axs is None:
            fig, axs = plt.subplots(4, 1, sharex=True, figsize=(6, 10))
        for k in range(len(self.channels)):
            self.df.plot(kind='line', x='time_offset', y=self.labels[k], ax=axs[k])
            axs[k].set_ylabel(self.labels[k])