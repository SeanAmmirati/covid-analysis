# -*- coding: utf-8 -*-
import logging
import git
import os
import pandas as pd
from datetime import datetime


def git_pull_dir(dir_):
    g = git.cmd.Git(dir_)
    g.pull()


class COVIDDataImporter:

    def __init__(self, git_dir='data/raw/external/COVID-19',
                 output_path='data/processed',
                 processed_filename='covid_processed_{scope}_{date}.csv'):
        self.git_dir = git_dir
        self.output_path = output_path
        self.processed_filename = processed_filename
        self.raw_dfs = None
        self.full_df = None

    def refresh_data(self):
        git_pull_dir(self.git_dir)

    def import_dfs(self):
        data_path = os.path.join(self.git_dir, 'csse_covid_19_data',
                                 'csse_covid_19_time_series')

        self.raw_dfs = {}

        for f in os.listdir(data_path):
            if os.path.splitext(f)[1] != '.csv':
                continue
            df_name = f.split('-')[-1].replace('.csv', '')
            df = pd.read_csv(os.path.join(data_path, f))

            self.raw_dfs[df_name] = df

        return self.raw_dfs

    def melt_data(self, autodownload=True):
        if not self.raw_dfs:
            if not autodownload:
                raise ValueError(
                    'You have not imported the data yet. To automatically do this, turn autodownload to True.')
            else:
                self.import_dfs()
        self.us_df = pd.DataFrame()
        self.global_df = pd.DataFrame()

        scope_to_header_cols = {
            'global': 4,
            'us': 11
        }

        for col, df in self.raw_dfs.items():
            scope = col.split('_')[-1].lower()
            header_n = scope_to_header_cols[scope]
            id_vars = df.columns[:header_n]

            value_vars = df.columns[header_n:]
            df = pd.melt(df, id_vars=id_vars, value_vars=value_vars,
                         var_name='Date', value_name=col)
            df.set_index(df.columns[:(header_n + 1)].tolist(), inplace=True)

            if scope == 'global':
                self.global_df = pd.concat([self.global_df, df], axis=1)
            elif scope == 'us':
                self.us_df = pd.concat([self.us_df, df], axis=1)
        return self.global_df, self.us_df

    def process_dfs(self):
        for df in [self.global_df, self.us_df]:
            if df is None:
                raise ValueError('DataFrame has not been merged yet.')

            self.process_df(df)

    def process_df(self, df):
        df.reset_index(inplace=True)

        df['Date'] = pd.to_datetime(self.full_df['Date'])

        long_names = df.columns[df.columns.str.contains(
            'time_series')]
        rename_dict = {x: x.split('_')[-2].title() for x in long_names}

        df.rename(columns=rename_dict, inplace=True)

        return df

    def _create_full_filename(self, dir_path, scope):
        date_str = str(datetime.today())
        fmt_dict = dict(date=date_str, scope=scope)
        formatted_filename = self.processed_filename.format(**fmt_dict)
        return os.path.join(dir_path, formatted_filename)

    def _return_df_by_scope_str(self, scope):
        if scope == 'global':
            return self.global_df
        elif scope == 'us':
            return self.us_df

    def save(self, output_path=None):
        if not output_path:
            output_path = self.output_path

        for scope in ['us', 'global']:

            full_output_path = self._create_full_filename(output_path, scope)
            df = self._return_df_by_scope_str(scope)
            df.to_csv(full_output_path)

    def load(self, input_path=None):
        if not input_path:
            input_path = self.output_path

        for scope in ['us', 'global']:
            full_input_path = self._create_full_filename(input_path, scope)
            setattr(self, f'{scope}_df', pd.read_csv(full_input_path))

        return self.us_df, self.global_df

    def process(self):
        try:
            return self.load()
        except FileNotFoundError:
            pass

        self.import_dfs()
        self.melt_data()
        self.process_dfs()
        self.save()
        return self.full_df


def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    covid = COVIDDataImporter()
    covid.process()
    print(covid.full_df)


if __name__ == '__main__':

    main()
