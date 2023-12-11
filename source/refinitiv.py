import os
import time
import configparser
import polars as pl
import pandas as pd
import eikon as ek

from itertools import islice
from tqdm import tqdm


class EikonHelper:
    def __init__(self, cell_limit=200_000, time_period=25, save_freq=1, n_batches=None):
        self.all_fields, self.all_instruments = self.get_metadata()
        self.save_path = os.path.join("assets", "Eikon", "data.parquet")

        self.cell_limit = cell_limit
        self.time_period = time_period
        self.save_freq = save_freq
        self.n_batches = n_batches
        self.instrument_limit = cell_limit // (len(self.all_fields) * self.time_period)            

        self.set_app_key()
        self.get_data()
        self.get_state()

    def set_app_key(self):
        config = configparser.ConfigParser()
        config.read(os.path.join(".config", "refinitiv.ini"))
        ek.set_app_key(config["RDP"]["app_key"])

    def get_metadata(self):
        meta_dir = os.path.join("assets", "metadata")
        with open(os.path.join(meta_dir, "fields.txt")) as f:
            fields = [x.strip("\n") for x in f]

        # # Get date fields
        # new_fields = []
        # for field in fields:
        #     new_fields.extend([field, field+".DATE"])

        with open(os.path.join(meta_dir, "instruments.txt")) as f:
            instruments = [x.strip("\n") for x in f]
        return fields, instruments

    def get_single_batch(self, instruments, fields=None):
        def ffill(c):
            return (pl
                .col(c)
                .fill_null(strategy="forward")
                .over("Instrument")
                .keep_name()
            )
        
        def fill_null(c):
            s = pl.col(c).cast(str)
            return pl.when(s=="").then(None).otherwise(s).keep_name()
        
        def date_func(c):
            s = c.str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%SZ", strict=False)
            return (pl
                .when(c.is_not_null())
                .then(s)
                .otherwise(None)
            )
        
        df = pl.from_pandas(ek.get_data(
            instruments=instruments,
            fields=fields if fields else self.all_fields,
            parameters={
                "Scale": 6,
                "SDate": 0,
                "EDate": -self.time_period,
                "FRQ": "FY",
                "Curn": "Native"
            },
            field_name=True
        )[0])

        return (df
            .lazy()
            .rename({k: k.strip() for k in df.columns})
            # .with_columns([fill_null(c) for c in EikonHelper.static_fields])
            # .with_columns([ffill(c) for c in EikonHelper.static_fields])
            # .with_columns([pl.col(c).cast(pl.Float64) for c in EikonHelper.dynamic_fields])
            # .with_columns(Date=date_func(pl.col("Date")))
        )
    
    def save_data(self):
        self.data.collect().write_parquet(self.save_path)

    def get_data(self):
        if os.path.exists(self.save_path):
            self.data = pl.read_parquet(self.save_path)
        else:
            self.data = self.get_single_batch(instruments=["AAPL.OQ", "AMZN.OQ"])
            self.save_data()
        self.data = self.data.lazy()
    
    def get_state(self):
        data = pl.read_parquet(self.save_path, columns=["Instrument"])
        self.retrieved_instruments = data["Instrument"].unique().to_list()
        self.remaining_instruments = list(set(self.all_instruments).difference(set(self.retrieved_instruments)))
        self.batches = self.get_batches(self.remaining_instruments, self.instrument_limit)

        print("=======================================")
        print("Cell limit:", self.cell_limit)
        print("Instrument limit:", self.instrument_limit)
        print("Tickers retrieved: %s/%s" % (len(self.retrieved_instruments), len(self.all_instruments)))
        print("Batches remaining:", len(self.batches))
        print("=======================================")

    def get_batches(self, it, size):
        it = iter(it)
        return list(iter(lambda: list(islice(it, size)), []))   

    def extract(self):
        batch = self.batches[:self.n_batches] if self.n_batches else self.batches
        for i, instruments in tqdm(enumerate(batch), total=len(batch)):
            try:
                df = self.get_single_batch(instruments=instruments)
                
                self.data = pl.concat([self.data, df])
                self.save_data()
                time.sleep(15)
            except Exception as e:
                print(e, instruments)
                continue


class Eikon2:
    def __init__(self):
        ek.set_app_key("fd36c20b01ed4ad6b3248805ae5031749349e3c9")
    
    def get_instruments(self, country):
        with open(r"assets\metadata\instruments.txt", "r") as f:
            instruments = [x.strip("\n") for x in f]

        retrieved_instruments = [x.strip(".csv") for x in os.listdir("assets\Eikon")]
        remaining_instruments = list(set(instruments).difference(retrieved_instruments))
        
        if country:
            country_instruments = pd.read_csv("assets\metadata\misc\REF_exchangecountry.csv")
            country_instruments = country_instruments[country_instruments["Country of Exchange"]==country]["Instrument"].tolist()
            return [instrument for instrument in remaining_instruments if instrument in country_instruments]
        else:
            return remaining_instruments
        
    def get_fields(self):
        with open(r"assets\metadata\fields.txt", "r") as f:
            fields = [x.strip("\n") for x in f]

        return fields
    
    def download_single_instrument(self, instrument):
        instrument_path = os.path.join("assets", "Eikon", f"{instrument}.csv")
        all_fields = self.get_fields()
        no_date_fields = [
            "TR.GICSINDUSTRY",
            "TR.HEADQUARTERSCOUNTRY",
            "TR.SICINDUSTRY",
            "TR.COMPANYREPORTCURRENCY",
            "TR.SICINDUSTRYCODE",
            "TR.SICINDUSTRYGROUPCODE",
            "TR.GICSSUBINDUSTRYCODE",
            "ISIN_CODE",
            "TR.GICSSECTORCODE",
            "TR.GICSINDUSTRYGROUP",
            "TR.EXCHANGECOUNTRY",
            "TR.COMPANYNAME",
            "TR.GICSINDUSTRYCODE",
            "TR.INSTRUMENTTYPE",
            "TR.GICSSECTOR",
            "TR.SICINDUSTRYGROUP",
            "TR.ISINCODE",
            "TR.GICSSUBINDUSTRY",
            "TR.TICKERSYMBOL",
            "TR.CUSIPCODE",
            "TR.REGISTRATIONCOUNTRY",
            "TR.GICSINDUSTRYGROUPCODE"
        ]
        date_fields = [field+".DATE" for field in all_fields if field not in no_date_fields]

        df, err = ek.get_data(
            instruments=[instrument],
            fields=all_fields+date_fields,
            parameters={
                "Scale": 6,
                "SDate": 0,
                "EDate": -25,
                "FRQ": "FY",
                "Curn": "Native"
            },
            field_name=True
        )        
        # for field in date_fields:
        #     print(field)
        #     df[field] = pd.to_datetime(df[field], format="%Y-%m-%dT%H:%M:%SZ")

        df.to_csv(instrument_path, index=False)

    def download_data(self, country=None):
        instruments = self.get_instruments(country)
        for instrument in tqdm(instruments):
            try:
                self.download_single_instrument(instrument)
            except:
                print(f"Failed: {instrument}")
                # time.sleep(15)
                continue
