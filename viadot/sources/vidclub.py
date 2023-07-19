import json
import urllib
import os
import requests
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from copy import deepcopy
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel

from ..config import get_source_credentials
from ..exceptions import CredentialError, ValidationError
from ..utils import handle_api_response
from ..utils import add_viadot_metadata_columns
from ..signals import SKIP
from .base import Source

class VidclubCredentials(BaseModel):
    url:str
    token:str

class Vidclub(Source):

    """
    Class implementing the vidclub API.
    Documentation for this API is located at: https://evps01.envoo.net/vipapi/.
    """

    def __init__(
        self,
        *args,
        credentials: VidclubCredentials = None,
        config_key: str = None,
        **kwargs,
    ):

        """
        Description:
            Create an instance of Vidclub.
        Args:
            credentials (VidclubCredentials): Credentials to vidclub.
            config_key (str, optional): The key in the viadot config holding relevant.
        """

        credentials = credentials or get_source_credentials(config_key)
        if credentials is None:
            raise CredentialError("Missing credentials.")
        
        if not credentials.get('url') and credentials.get('token'):
            raise CredentialError("'url' and 'token' credentials are required")

        validated_creds = dict(VidclubCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.url = self.credentials['url']
        self.token = self.credentials['token']

        self.headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/json",
        }

    def build_query(
        self,
        source: Literal["jobs", "product", "company", "survey"],
        from_date: str,
        to_date: str,
        items_per_page: int
    )->str:
        """
        Builds the query from the inputs.

        Args:
            source (str): The endpoint source to be accessed, has to be among these:
                ['jobs', 'product', 'company', 'survey'].
            from_date (str): Start date for the query.
            to_date (str): End date for the query, if empty, datetime.today() will be used.
            api_url (str): Generic part of the URL.
            items_per_page (int): number of entries per page.

        Returns:
            str: Final query with all filters added.

        Raises:
            ValidationError: If any source different than the ones in the list are used.
        """
        if source in ["jobs", "product", "company"]:
            url = f"{self.url}{source}?from={from_date}&to={to_date}&limit={items_per_page}"
        elif source in "survey":
            url = f"{self.url}{source}?language=en&type=question"
        else:
            raise ValidationError(
                "Pick one these sources: jobs, product, company, survey"
            )
        return url
    
    def get_response(
        self,
        source: Literal["jobs", "product", "company", "survey"],
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region="null",
    ) -> pd.DataFrame:
        """
        Gets the response from the API queried and transforms it into DataFrame.

        Args:
            source (str): The endpoint source to be accessed, has to be among these:
                ['jobs', 'product', 'company', 'survey'].
            from_date (str, optional): Start date for the query, by default is the oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default, datetime.today() will be used.
            items_per_page (int, optional): Number of entries per page. 100 entries by default.
            region (str, optinal): Region filter for the query. By default, it is empty.

        Returns:
            pd.DataFrame: Table of the data carried in the response.

        Raises:
            ValidationError: If any source different than the ones in the list are used.
            ValidationError: If the initial date of the query is before the oldest date in the data (2023-03-22).
            ValidationError: If the final date of the query is before the start date.
        """

        # Dealing with bad arguments
        if source not in ["jobs", "product", "company", "survey"]:
            raise ValidationError(
                "The source has to be: jobs, product, company or survey"
            )
        if to_date == None:
            datetime.today().strftime("%Y-%m-%d")

        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
        oldest_date_obj = datetime.strptime("2022-03-22", "%Y-%m-%d")
        delta = from_date_obj - oldest_date_obj

        if delta.days < 0:
            raise ValidationError("from_date cannot be earlier than 2023-03-22!!")

        to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
        delta = to_date_obj - from_date_obj

        if delta.days < 0:
            raise ValidationError("to_date cannot be earlier than from_date!")

        # Preparing the Query
        first_url = self.build_query(
            source,
            from_date,
            to_date,
            # self.credentials["url"],
            items_per_page,
        )
        headers = self.headers

        # Getting first page
        response = handle_api_response(url=first_url, headers=headers, method="GET")

        # Next Pages
        response = response.json()

        if isinstance(response, dict):
            keys_list = list(response.keys())
        elif isinstance(response, list):
            keys_list = list(response[0].keys())
        else:
            keys_list = []

        if "data" in keys_list:
            # first page content
            df = pd.DataFrame(response["data"])
            length = df.shape[0]
            page = 2

            while length == items_per_page:
                url = f"{first_url}&page={page}"
                r = handle_api_response(url=url, headers=headers, method="GET")
                response = r.json()
                df_page = pd.DataFrame(response["data"])
                if source == "product":
                    df_page = df_page.transpose()
                length = df_page.shape[0]
                df = pd.concat((df, df_page), axis=0)
                page += 1

        else:
            df = pd.DataFrame(response)

        return df


    @add_viadot_metadata_columns
    def to_df(
        self,
        source:str = "",
        from_date:str = "",
        to_date:str = "",
        limit:int = 10,
        if_empty: str = "fail"
    )-> pd.DataFrame:

        """
        Description:
            Get the response from the API queried and transforms it into DataFrame.
        Args:
            source(type=str,default_value=""):The endpoint source to be accessed, has to be among these: ['jobs', 'product', 'company', 'survey']..
            from_date(type=str,default_value=""):Must be Y-m-d format of date and it can not be bigger from "to" parameter. Can not be empty or null.
            to_date(type=str,default_value=""):Must be Y-m-d format of date and it can not be smaller from "from" parameter. Can not be empty or null.
            limit(type=Optional(int),default_value=10):Route support pagination, this parameter is optional, default is 10..
        Returns:
            pd.DataFrame: Table of the data carried in the response.
        """

        df = self.get_response(source=source,from_date=from_date,to_date=to_date,items_per_page=limit)

        if df.empty:
            try:
                self._handle_if_empty(if_empty)
            except SKIP:
                return pd.DataFrame()
            
        return df
