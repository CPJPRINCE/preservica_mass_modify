from preservica_modify.common import fmthelper
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

class DataFrameHandler:
    def __init__(self, input_file: str, column_sensitivity: bool = False):
        self.input_file = input_file
        self.column_sensitivity = column_sensitivity
        self.df = None

    def _set_input_flags(self) -> None:
        """
        Sets the input flags
        """
        self.title_flag = False
        self.description_flag = False
        self.security_flag = False
        self.retention_flag = False
        self.dest_flag = False
        self.move_flag = False
        if self.TITLE_FIELD in self.column_headers:
            self.title_flag = True
        if self.DESCRIPTION_FIELD in self.column_headers:
            self.description_flag = True
        if self.SECURITY_FIELD in self.column_headers:
            self.security_flag = True
        if self.RETENTION_FIELD in self.column_headers:
            self.retention_flag = True
        if self.MOVETO_FIELD in self.column_headers:
            self.move_flag = True
        logger.debug(f'Input Flags - Title: {self.title_flag}, Description: {self.description_flag}, Security: {self.security_flag}, Retention: {self.retention_flag}, Move: {self.move_flag}')

    def init_df(self) -> None:

        input_fmt = fmthelper(os.path.splitext(self.input_file)[-1].replace('.',''))
        logger.info(f'Initializing dataframe from input file: {self.input_file} with detected format: {input_fmt}. May take time to load.')

        if input_fmt.endswith("xlsx"):
            self.df: pd.DataFrame = pd.read_excel(self.input_file)
        elif input_fmt.endswith("csv"):
            self.df: pd.DataFrame = pd.read_csv(self.input_file)
        elif input_fmt.endswith("ods"):
            self.df: pd.DataFrame = pd.read_excel(self.input_file, engine='odf')
        elif input_fmt.endswith("json"):
            self.df: pd.DataFrame = pd.read_json(self.input_file, orient='index')
        elif input_fmt.endswith("xml"):
            self.df: pd.DataFrame = pd.read_xml(self.input_file)
        else:
            logger.error("Unsupported file type for input. Please use .xlsx, .csv, .json or .xml")
            raise ValueError("Unsupported file type for input. Please use .xlsx, .csv, .json or .xml")
       
        self.column_headers = list(self.df.columns.values)
        if self.column_sensitivity is True:
            self.column_headers = [str(header).lower() for header in self.column_headers]
            self.df.columns = self.column_headers
        date_headers = [header for header in self.column_headers if "date" in str(header).lower()]
        self.df[date_headers] = self.df[date_headers].apply(lambda x: pd.to_datetime(x,format='mixed'))
