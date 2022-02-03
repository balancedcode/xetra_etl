"""
ETL Commonent for Detusche Boerse XETRA
"""
from typing import NamedTuple

from xetra.common.s3 import S3BucketConnector
import logging

class XetraSourceConfig(NamedTuple):
    """
    Class for source config. data

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volume in source
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str

class XetraTargetConfig(NamedTuple):
    """
    Class for target config. data

    trg_col_isin: column name for ISIN in target
    trg_col_date: column name for date in target
    trg_col_open_price: column name for opening price in target
    trg_col_close_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_daily_trade_vol: column name for daily traded volume in target
    trg_col_change_last_close: column name for change to previous days closing price in target
    trg_key: basic key for target file
    trg_key_date_format: date format for target file key
    trg_format: file format for target file
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_open_price: str
    trg_col_close_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_daily_trade_vol: str
    trg_col_change_last_close: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class XetraETL():
    """
    Reads the Xetra data and transforms and writes the transformed data to the target
    """

    def __init__(self,
                 s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector,
                 meta_key: str,
                 src_args: XetraSourceConfig,
                 trg_args: XetraTargetConfig):
        """
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to S3 source bucket
        :param s3_bucket_trg: connection to S3 target bucket
        :param meta_key: used as self-meta_key -> key for meta file (dates processed)
        :param src_args: NamedTuple class with source config. file
        :param trg_args: NamedTuple class with target config. file
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date =
        self.extract_date_list =
        self.meta_update_list =

    def extract(self):
        pass

    def transform_report1(self):
        pass

    def load(self):
        pass

    def etl_report1(self):
        pass

