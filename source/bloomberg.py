"""
https://mengjiexu.com/post/bloomberg-api/
https://capm.pe/bloomberg/
"""
import os
import copy
import pandas as pd
import regex as re

from xbbg import blp
from loguru import logger
from tqdm.notebook import tqdm
from glob import glob


class BaseExtractor:
    dir = "assets"
    regions = ['EU', 'ASIA', 'OTHERS']  
    flds = ['GHG_SCOPE_1', 'GHG_SCOPE_2', 'GHG_SCOPE_3'] 

    def __init__(self) -> None:
        self._create_folders() 

    def _get_tickers_by_region(self, region, flds):
        src_path = os.path.join(BaseExtractor.dir, f"{flds}_{region}.xlsx")
        self._ticker_df = pd.read_excel(src_path)
        return self._ticker_df['Ticker'].unique().tolist()


    def _create_folders(self):
        for region in BaseExtractor.regions:
            for fld in BaseExtractor.flds:
                region_folder = os.path.join(BaseExtractor.dir, fld, region)
                if not os.path.exists(region_folder):
                    os.makedirs(region_folder, exist_ok=True)
                    logger.info(f"{fld}/{region} folder created.")


class BDH(BaseExtractor):
    def __init__(
            self, region, flds, daily_limit=90, start_date='2011-01-01', 
            end_date='2023-01-01', freq='Y'):
        super().__init__()
        assert region in BDH.regions, f"Please input region only from {BDH.regions}"
        self._region = region
        self._folder_dir = os.path.join(BDH.dir, region)
        self._tickers = self._get_tickers_by_region(region, flds)
        self._n_tickers = len(self._tickers)
        self._remaining_tickers = self._get_remaining_tickers()
        self._n_remaining_tickers = len(self._remaining_tickers)
        self._flds = flds
        self._n_flds = 1
        self._daily_limit = daily_limit     
        self._periods = pd.date_range(start=start_date, end=end_date, freq='Y')
        self._years = [d.year for d in self._periods]
        self._n_periods = len(self._periods)
        self._cells_per_ticker = self._n_flds*self._n_periods
        self._batch_size = self._get_batch_size()
        self._days_remaining = (
            self._n_remaining_tickers/self._batch_size if self._n_remaining_tickers>0 else 0)
        self._start_date = start_date
        self._end_date = end_date
        self._freq = freq
    

    def _get_batch_size(self):
        a = self._daily_limit//self._cells_per_ticker
        return a if self._n_remaining_tickers>a else self._n_remaining_tickers
    

    def __repr__(self):
        return "\n".join([
            "-----Summary-----",
            f"region: {self._region}",
            f"n_remaining_tickers: {self._n_remaining_tickers}/{self._n_tickers}",
            f"flds: {self._flds}",
            f"n_flds: {self._n_flds}",            
            f"daily_limit: {self._daily_limit}",
            f"cells_per_ticker: {self._cells_per_ticker}",
            f"days_remaining: {self._days_remaining}",
            f"batch_size: {self._batch_size}",
            f"start_date: {self._start_date}",
            f"end_date: {self._end_date}",
            f"freq: {self._freq}",
            f"n_periods: {self._n_periods}",
        ])
    

    def _get_single_ticker_data(self, ticker):
        return blp.bdh(
            tickers=ticker,
            flds=self._flds,
            start_date=self._start_date,
            end_date=self._end_date,
            Per=self._freq,
            Fill='NA')


    def _get_remaining_tickers(self):
        extracted_ticker_paths = glob(os.path.join(self._folder_dir, "*.csv"))
        extracted_tickers = [
            os.path.splitext(os.path.basename(ticker))[0] for ticker in extracted_ticker_paths]
        remaining_tickers = copy.deepcopy(self._tickers)
        for ticker in self._tickers:
            temp_ticker = re.sub("(\/| )", "_", ticker)
            if temp_ticker in extracted_tickers:
                remaining_tickers.remove(ticker)
        return remaining_tickers


    def _get_save_path(self, ticker):
        file_name = re.sub("(\/| )", "_", ticker) + ".csv"
        return os.path.join(self._folder_dir, file_name)
        

    def fetch(self):
        if not self._remaining_tickers:
            # Early stop
            logger.info("Region extracted.")
            return

        # Parse remaining tickers
        not_empty_count = 0
        for i, ticker in tqdm(
                enumerate(self._remaining_tickers[:self._batch_size]), total=self._batch_size):
            save_path = self._get_save_path(ticker)
            if not os.path.exists(save_path):
                try:
                    row_data = self._get_single_ticker_data(ticker)
                    row_data.to_csv(save_path)
                    if not row_data.empty:
                        not_empty_count+=1
                        print(f"Not Empty: {not_empty_count}/{i+1}")
                except:
                    logger.info(f"Unable to save for {ticker} to {save_path}")


if __name__ == "__main__":
    fn = BDH(
        region='Europe',
        flds=['GHG_SCOPE_1', 'GHG_SCOPE_2', 'GHG_SCOPE_3'],
        daily_limit=90,
        start_date='2010-01-01',
        end_date='2020-01-01',
        freq='Y')
    print(fn)
    fn.fetch()