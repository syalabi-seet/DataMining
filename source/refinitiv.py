import os
import time
import configparser
import polars as pl
import refinitiv.data.eikon as ek

from itertools import islice
from tqdm.notebook import tqdm


class EikonHelper:
    static_fields = [
        "Company Name", "Country of Incorporation", "Country of Exchange", 
        "Country of Headquarters", "Instrument Type", "ISIN", "ISIN Code",
        "ISIN_CODE", "SEDOL", "CUSIP Code", "SIC Industry Group Code",
        "SIC Industry Group Name", "SIC Industry Code", "SIC Industry Name", 
        "GICS Sector Code", "GICS Sector Name", "GICS Industry Group Code", 
        "GICS Industry Group Name", "GICS Industry Code", "GICS Industry Name",
        "GICS Sub-Industry Code", "GICS Sub-Industry Name", "Ticker Symbol", 
        "Currency Code"
    ]
    dynamic_fields = [
        'Total Assets',
        'Income before Discontinued Operations & Extraordinary Items',
        'Preferred Shareholders Equity',
        'Total Liabilities',
        'Upstream scope 3 emissions Fuel- and Energy-related Activities',
        'Income Taxes - Payable - Short-Term',
        'Short-Term Debt & Current Portion of Long-Term Debt',
        'Interest Expense - Net of Capitalized Interest',
        "Total Shareholders' Equity incl Minority Intr & Hybrid Debt",
        'Net Income before Minority Interest',
        'Depreciation Depletion & Amortization - Cash Flow',
        'Upstream scope 3 emissions Leased Assets',
        'Upstream scope 3 emissions Employee Commuting',
        'Minority Interest - Equity',
        'Upstream scope 3 emissions Capital goods',
        "Shareholders' Equity - Attributable to Parent ShHold - Total",
        'Downstream scope 3 emissions Other',
        'Income before Taxes',
        'Inventories - Total',
        'Preferred Stock - Redeemable - Total',
        'Equity Earnings/(Loss) before Taxes including Non-Recurring',
        'Minority Interest',
        'Selling General & Administrative Expenses - Total',
        'Advertising Expense',
        'Company Shares',
        'Upstream scope 3 emissions Other',
        'Earnings before Interest Taxes Depreciation & Amortization',
        'Research & Development Expense',
        'Income Taxes',
        'Trade Account Payables - Total',
        'Downstream scope 3 emissions End-of-life Treatment of Sold Products',
        'EPS - Diluted - incl Extraordinary Items, Common - Total',
        'Deferred Tax & Investment Tax Credits - Long-Term',
        'Downstream scope 3 emissions Processing of Sold Products',
        'EPS - Diluted - excl Extraordinary Items - Normalized -Total',
        'Estimated CO2 Equivalents Emission Total',
        'Cash & Short Term Investments - Total',
        'Dividends - Common - Cash Paid',
        'Market Capitalization',
        'Extraordinary Activities - after Tax - Gain/(Loss)',
        'Total Current Assets',
        'Dividends - Preferred - Cash Paid',
        'Net Income after Minority Interest',
        'Deferred Tax - Liability - Long-Term',
        'Cost Of Goods Sold - Actual',
        'Downstream scope 3 emissions Use of Sold Products',
        'Upstream scope 3 emissions Business Travel',
        'Total Current Liabilities',
        'Upstream scope 3 emissions Transportation and Distribution',
        'EPS - Diluted - excl Exord Items Applicable to Common Total',
        'Interest Income - Non-Bank',
        'Downstream scope 3 emissions Leased Assets',
        'Loans & Receivables - Total',
        'Non-Recurring Income/(Expense) - Total',
        'EPS - Basic - excl Extraordinary Items - Normalized - Total',
        'Labor & Related Expenses - Total',
        'Revenue from Business Activities - Total',
        'Depreciation & Amortization - Supplemental',
        'Liquidation & Redemption Value of Redeem Pref Stock (Eq)',
        'Common Equity Attributable to Parent Shareholders',
        'Net Cash Flow from Operating Activities',
        'Downstream scope 3 emissions Transportation and Distribution',
        'Earnings before Interest & Taxes (EBIT)',
        'CO2 Equivalent Emissions Indirect, Scope 2',
        'Capital Expenditures - Total',
        'Price Close',
        'Operating Profit before Non-Recurring Income/Expense',
        'Downstream scope 3 emissions Franchises',
        'Income available to Common excluding Extraordinary Items',
        'Sale of Tangible & Intangible Fixed Assets - Gain/(Loss)',
        'Upstream scope 3 emissions Purchased goods and services',
        'Downstream scope 3 emissions Investments',
        'Debt - Long-Term - Total',
        'Dividend Per Share - Actual',
        'Common Shares - Outstanding - Total',
        'Other Non-Operating Income/(Expense) - Total',
        'CO2 Equivalent Emissions Direct, Scope 1',
        'EPS - Basic - excl Extraordinary Items, Common - Total',
        'Upstream scope 3 emissions Waste Generated in Operations',
        'Property Plant & Equipment - Net - Total',
        'Cost of Revenues - Unclassified'
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
        meta_dir = os.path.join("assets", "metadata",)
        with open(os.path.join(meta_dir, "fields.txt")) as f:
            fields = [x.strip("\n") for x in f]

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
            }
        )[0])

        return (df
            .lazy()
            .rename({k: k.strip() for k in df.columns})
            .with_columns([fill_null(c) for c in EikonHelper.static_fields])
            .with_columns([ffill(c) for c in EikonHelper.static_fields])
            .with_columns([pl.col(c).cast(pl.Float64) for c in EikonHelper.dynamic_fields])
            .with_columns(Date=date_func(pl.col("Date")))
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
            except Exception as e:
                print(e, instruments)
                continue
        return self.save_data()