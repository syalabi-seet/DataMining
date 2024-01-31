import os
import time
import configparser
import polars as pl
import pandas as pd
import eikon as ek

from itertools import islice
from tqdm import tqdm


class EikonHelper:
    no_date_fields = [
        # "TR.GICSINDUSTRY",
        # "TR.HEADQUARTERSCOUNTRY",
        # "TR.SICINDUSTRY",
        # "TR.COMPANYREPORTCURRENCY",
        # "TR.SICINDUSTRYCODE",
        # "TR.SICINDUSTRYGROUPCODE",
        # "TR.GICSSUBINDUSTRYCODE",
        # "ISIN_CODE",
        # "TR.GICSSECTORCODE",
        # "TR.GICSINDUSTRYGROUP",
        "TR.EXCHANGECOUNTRY",
        # "TR.COMPANYNAME",
        # "TR.GICSINDUSTRYCODE",
        # "TR.INSTRUMENTTYPE",
        # "TR.GICSSECTOR",
        # "TR.SICINDUSTRYGROUP",
        "TR.ISINCODE",
        # "TR.GICSSUBINDUSTRY",
        "TR.TICKERSYMBOL",
        # "TR.CUSIPCODE",
        # "TR.REGISTRATIONCOUNTRY",
        # "TR.GICSINDUSTRYGROUPCODE"
    ]

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
        # with open(os.path.join(meta_dir, "fields.txt")) as f:
        #     fields = list(set([x.strip("\n") for x in f]))
        fields = [
            "TR.TICKERSYMBOL",
            "TR.ISINCODE",
            "TR.EXCHANGECOUNTRY",
            "TR.CO2EMISSIONTOTAL", 
            "TR.CO2EMISSIONTOTAL.DATE", 
            "TR.CO2DIRECTSCOPE1", 
            "TR.CO2DIRECTSCOPE1.DATE",
            "TR.CO2INDIRECTSCOPE2",
            "TR.CO2INDIRECTSCOPE2.DATE",
            "TR.CO2ESTIMATIONMETHOD",
            "TR.CO2ESTIMATIONMETHOD.DATE",
            "TR.ANALYTICESTIMATEDCO2TOTAL",
            "TR.ANALYTICESTIMATEDCO2TOTAL.DATE",
            "TR.CO2INDIRECTSCOPE3",
            "TR.CO2INDIRECTSCOPE3.DATE",
            "TR.ANALYTICCO2",
            "TR.ANALYTICCO2.DATE"
        ]

        with open(os.path.join(meta_dir, "instruments.txt")) as f:
            instruments = [x.strip("\n") for x in f]
        return fields, instruments

    def get_single_batch(self, instruments, fields=None):        
        df = ek.get_data(
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
        )[0]

        for field in [c for c in df.columns if c.endswith(".DATE")]:
            df[field] = pd.to_datetime(df[field], format=r"%Y-%m-%dT%H:%M:%SZ")

        # str_cols = [
        #     "TR.SEDOL",
        #     "ISIN_CODE",
        #     "TR.TICKERSYMBOL",
        #     "TR.CUSIPCODE",
        # ]
        # for col in str_cols:
        #     df[col] = df[col].astype(str)

        # df["TR.F.EPSDILEXCLEXORDITEMSCOMTOT"] = df["TR.F.EPSDILEXCLEXORDITEMSCOMTOT"].astype(float)
        return df
    
    def save_data(self):
        self.data.to_parquet(self.save_path)

    def get_data(self):
        if os.path.exists(self.save_path):
            self.data = pd.read_parquet(self.save_path)
        else:
            self.data = self.get_single_batch(instruments=["AAPL.OQ", "AMZN.OQ"])
            self.save_data()
    
    def get_state(self):
        data = pd.read_parquet(self.save_path, columns=["Instrument"])
        self.retrieved_instruments = data["Instrument"].unique().tolist()
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
                self.data = pd.concat([self.data, df], axis=0)
                self.save_data()
                time.sleep(15)
            except Exception as e:
                print(e, instruments)
                continue