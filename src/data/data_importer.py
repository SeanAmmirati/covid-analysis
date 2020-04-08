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
                 processed_filename='covid_processed_{date}.csv'):
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
        self.full_df = pd.DataFrame()
        for col, df in self.raw_dfs.items():
            id_vars = df.columns[:4]
            value_vars = df.columns[4:]
            df = pd.melt(df, id_vars=id_vars, value_vars=value_vars,
                         var_name='Date', value_name=col)
            df.set_index(df.columns[:5].tolist(), inplace=True)
            self.full_df = pd.concat(
                [self.full_df, df], axis=1)
        return self.full_df

    def process_df(self):
        if self.full_df is None:
            raise ValueError('DataFrame has not been merged yet.')

        self.full_df.reset_index(inplace=True)
        self.full_df['Date'] = pd.to_datetime(self.full_df['Date'])

        long_names = self.full_df.columns[self.full_df.columns.str.contains(
            'time_series')]
        rename_dict = {x: x.split('_')[-2].title() for x in long_names}

        self.full_df.rename(columns=rename_dict, inplace=True)

        return self.full_df

    def _create_full_filename(self, dir_path):
        date_str = str(datetime.today())
        fmt_dict = dict(date=date_str)
        formatted_filename = self.processed_filename.format(**fmt_dict)
        return os.path.join(dir_path, formatted_filename)

    def save(self, output_path=None):
        if not output_path:
            output_path = self.output_path
        full_output_path = self._create_full_filename(output_path)
        self.full_df.to_csv(full_output_path)

    def load(self, input_path=None):
        if not input_path:
            input_path = self.output_path

        full_input_path = self._create_full_filename(input_path)
        self.full_df = pd.read_csv(full_input_path)

        return self.full_df

    def process(self):
        try:
            return self.load()
        except FileNotFoundError:
            pass

        self.import_dfs()
        self.melt_data()
        self.process_df()
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
