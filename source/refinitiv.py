"""
https://pypi.org/project/refinitiv-data/
"""
import os
import time
import json
import pandas as pd
import refinitiv.data.eikon as ek

from tqdm.notebook import tqdm
from itertools import islice
from loguru import logger


## Helper functions
def match_years(file_path):
    save_path = os.path.splitext(file_path)[0] + "_matched.csv"
    if os.path.exists(save_path):
        return

    df = pd.read_csv(
        file_path, 
        low_memory=False, 
        parse_dates=['TotAssets Date', 'PriceClose Date'], 
        infer_datetime_format=True)

    df1 = df.drop(columns=['PriceClose Date', 'Price Close'])
    df1.insert(2, 'TotAssets.Year', df1['TotAssets Date'].dt.year)

    # Pop out PriceClose columns
    df2 = df[['Instrument', 'PriceClose Date', 'Price Close']]
    df2.insert(2, 'PriceClose.Year', df2['PriceClose Date'].dt.year)

    # Left join PriceClose sub-frame to main frame
    df3 = pd.merge(
        df1, 
        df2, 
        left_on=['Instrument', 'TotAssets.Year'], 
        right_on=['Instrument', 'PriceClose.Year'], 
        how='left')

    # Shift PriceClose columns position
    PriceClose_year = df3.pop("PriceClose.Year")
    PriceClose_date = df3.pop("PriceClose Date")
    df3.insert(3, 'PriceClose.Year', PriceClose_year)
    df3.insert(3, 'PriceClose Date', PriceClose_date)
    
    df3.to_csv(save_path, index=False)


class EikonHelper:
    def __init__(
            self, 
            region, 
            cell_limit_per_request=100_000, 
            time_period=25,
            sleep_duration=1
            ):      

        self._set_app_key()
        self.region = region
        self.cell_limit_per_request = cell_limit_per_request
        self.time_period = time_period
        self.sleep_duration = sleep_duration
        self.tickers = self._get_tickers()
        self.fields = self._get_fields()
        self.instrument_limit_per_request = (
            self.cell_limit_per_request // (len(self.fields) * self.time_period))
        self.data = self._get_data()
        self.sub_tickers = self._get_sub_tickers()        
        self.batch_tickers = self._get_batch(
            self.sub_tickers, self.instrument_limit_per_request)
        self._get_summary()

    def _set_app_key(self):
        with open(".config/refinitiv-data.config.json", 'r') as f:
            app_key = json.load(f)['sessions']['platform']['rdp']['app-key']
        ek.set_app_key(app_key)

    def _get_fields(self):
        with open("assets\metadata\eikon_fields.txt", "r") as f:
            fields = [x.strip("\n") for x in f]
        return fields

    def _get_tickers(self):
        with open("assets\metadata\eikon_tickers.json", "r") as f:
            tickers = json.load(f)
        return tickers

    def _get_schema(self, empty_path):
        batch_data, _ = ek.get_data(
            self.tickers[0], 
            fields=self.fields,
            parameters={
                'Scale': 0, 'SDate': 0, 'EDate': -self.time_period, 'FRQ': 'FY'})

        batch_data.columns.values[1] = 'TotAssets.Date'
        batch_data.columns.values[2] = 'Price Close.Date'
        batch_data.columns.values[3] = 'MktCap.Date'
        batch_data.columns.values[4] = 'CompanySharesOutstanding.Date'  
        empty_schema = pd.DataFrame(columns=batch_data.columns)
        empty_schema.to_parquet(empty_path)
        return empty_schema

    def _get_data(self):
        self.save_path = os.path.join("assets", "Eikon", f"{self.region.lower()}_data.parquet")
        if os.path.exists(self.save_path):
            data = pd.read_parquet(self.save_path)
        else:
            empty_path = os.path.join("assets", "metadata", "empty_eikon_data.parquet")
            if not os.path.exists(empty_path):
                data = self._get_schema(empty_path)
            else:
                data = pd.read_parquet(empty_path)
        return data

    def _get_sub_tickers(self):
        self.retrieved_tickers = list(self.data['Instrument'].unique())
        sub_tickers = [
            i for i in self.tickers[self.region] 
            if i not in self.retrieved_tickers]
        return sub_tickers

    def _get_batch(self, it, size):
        it = iter(it)
        return list(iter(lambda: tuple(islice(it, size)), ()))

    def _get_summary(self):
        print("=======================================")
        print("Region:", self.region)
        print("Cell limit per request:", self.cell_limit_per_request)
        print("Instrument limit per request:", self.instrument_limit_per_request)
        print(f"Tickers retrieved: {len(self.retrieved_tickers)}/{len(self.tickers[self.region])}")
        print("=======================================")

    def extract_single(self, tickers):
        batch_data, _ = ek.get_data(
            list(tickers),
            fields=self.fields,
            parameters={'Scale': 0, 'SDate': 0, 'EDate': -self.time_period, 'FRQ': 'FY'})

        batch_data.columns.values[1] = 'TotAssets.Date'
        batch_data.columns.values[2] = 'Price Close.Date'
        batch_data.columns.values[3] = 'MktCap.Date'
        batch_data.columns.values[4] = 'CompanySharesOutstanding.Date'

        if 'Instrument' not in batch_data.columns.tolist():
            logger.info(f"Empty dataframe: {tickers}")
            return

        self.data = pd.concat([self.data, batch_data], ignore_index=True)
        self.data = self.data.astype(str)
        self.data.to_parquet(self.save_path)
        time.sleep(self.sleep_duration)

    def extract(self):
        if not self.batch_tickers:
            logger.info(f"{self.region} completed")
            return

        for tickers in tqdm(self.batch_tickers):
            try:
                self.extract_single(tickers)
            except Exception as e:
                print(e)
                continue


if __name__ == "__main__":
    eikon_helper = EikonHelper(region="Europe")