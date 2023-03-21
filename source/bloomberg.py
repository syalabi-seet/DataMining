"""
https://mengjiexu.com/post/bloomberg-api/
https://capm.pe/bloomberg/
"""
import os
import pandas as pd

from xbbg import blp
from loguru import logger
from tqdm.notebook import tqdm
from glob import glob

from itertools import islice
from loguru import logger
import time

class BBHelper:
    regions = ['Asia', 'Europe', 'Others']
    fields = [
        'GHG_SCOPE_1', 
        'GHG_SCOPE_2',
        'GHG_SCOPE_3',
        'ENERGY_CONSUMPTION',
        'TOTAL_WATER_USE'
        'TOTAL_WASTE']

    def __init__(
            self, 
            region, 
            field,
            cell_limit_per_request=1_000,
            sleep_duration=60):      

        assert region in BBHelper.regions, f"Select region within {BBHelper.regions}"
        assert field in BBHelper.fields, f"Select field within {BBHelper.fields}"
        self.region = region
        self.field = field
        self.time_period = len(pd.date_range("2010-01-01", "2023-01-01", freq="Y"))
        self.cell_limit_per_request = cell_limit_per_request
        self.ticker_limit_per_request = self.cell_limit_per_request // self.time_period
        self.sleep_duration = sleep_duration
        
        self.tickers = self._get_tickers()
        self.data = self._get_saved_data()
        self.sub_tickers = self._get_sub_tickers()
        self.batch_tickers = self._get_batch(
            self.sub_tickers, self.ticker_limit_per_request)
        self._get_summary()


    def _get_tickers(self):
        self.dir = os.path.join("assets", "Bloomberg")
        path = os.path.join(self.dir, "Snapshots", f"{self.field}.xlsx")
        df = pd.read_excel(path, sheet_name=self.region)
        return df['Ticker'].unique().tolist()


    def _get_saved_data(self):
        self.save_dir = os.path.join(self.dir, self.field)
        if not os.path.exists(self.save_dir):
            os.mkdir(self.save_dir)

        self.save_path = os.path.join(self.save_dir, f"{self.region}.parquet")
        if os.path.exists(self.save_path):
            df = pd.read_parquet(self.save_path)
        else:
            df = pd.DataFrame(columns=['tic', 'datadate', self.field])
        return df


    def _get_sub_tickers(self):
        self.retrieved_tickers = self.data['tic'].unique().tolist()
        sub_tickers = [
            i for i in self.tickers
            if i not in self.retrieved_tickers]
        return sub_tickers


    def _get_batch(self, it, size):
        it = iter(it)
        return list(iter(lambda: tuple(islice(it, size)), ()))


    def _get_summary(self):
        print("=======================================")
        print("Region:", self.region)
        print("Cell limit per request:", self.cell_limit_per_request)
        print("Ticker limit per request:", self.ticker_limit_per_request)
        print("Tickers retrieved: {0}/{1}".format(len(self.retrieved_tickers), len(self.tickers)))
        print("Batches:", len(self.batch_tickers))
        print("Time periods:", self.time_period)
        print("=======================================")


    def extract(self):
        if not self.batch_tickers:
            logger.info(f"{self.region} completed")
            return

        for tickers in tqdm(self.batch_tickers):
            batch_data = blp.bdh(
                tickers=tickers,
                flds=self.field,
                start_date="2010-01-01",
                end_date="2023-01-01", 
                Per="Y",
                Fill="NA") 
            for ticker in tickers:
                if ticker not in batch_data.columns:
                    logger.info(f"{ticker} not available")
                    continue
                sub_data = batch_data[ticker].reset_index(names="datadate")
                sub_data['tic'] = ticker
                sub_data = sub_data.dropna(subset=self.field)
                self.data = pd.concat([self.data, sub_data])                 

            self.data.to_parquet(self.save_path)
            time.sleep(self.sleep_duration)
