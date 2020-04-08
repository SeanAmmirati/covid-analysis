import pandas as pd


class FeatureDataset:

    def __init__(self, df):
        self.df = df

    def create_features(self):

        self.df['Active ']