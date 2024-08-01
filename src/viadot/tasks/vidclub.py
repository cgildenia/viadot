
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional

from prefect import task, get_run_logger
from prefect_viadot.exceptions import MissingSourceCredentialsError
from prefect_viadot.utils import get_credentials

from viadot.sources import Vidclub

def convert_keys_to_lowercase(dictionary):
    new_dict = {}
    for key, value in dictionary.items():
        new_key = key.lower()
        new_dict[new_key] = value
    return new_dict

def remove_prefix_from_keys(dictionary, prefix):
    new_dict = {}
    for key, value in dictionary.items():
        if key.startswith(prefix):
            new_key = key[len(prefix):]  # Remove the prefix
        else:
            new_key = key
        new_dict[new_key] = value
    return new_dict

@task
def vidclub_to_df(
    config_key: str = None,
    credentials_secret: str = None,
    credentials: dict = None,
    source: str = '',
    region: str = '',
    from_date: str = '',
    to_date: str = '',
    if_empty: str = "fail"
) -> pd.DataFrame:

    """
    Description:
        Task run method.
    Args:
        credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the credentials. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
        credentials (dict, optional): The credentials as a dictionary. Defaults
            to None.
        source (type=str, default_value=): The endpoint source to be accessed. Defaults to None..
        region (type=str, default_value=): Region filter for the query. Defaults to None (parameter is not used in url). [December 2023 status: value 'all' does not work for company and jobs].
        from_date (type=str, default_value=): Start date for the query..
        to_date (type=str, default_value=): End date for the query, if empty, will be executed as datetime.today().strftime('%Y-%m-%d')..

    Returns:
        pd.DataFrame: Table of the data carried in the response.
    """
    logger = get_run_logger()

    if not (credentials_secret or config_key or credentials):
        raise MissingSourceCredentialsError
    credentials = credentials or get_credentials(credentials_secret)
    credentials = convert_keys_to_lowercase(credentials)
    credentials = remove_prefix_from_keys(credentials, "azure_")

    vidclub = Vidclub(credentials=credentials, config_key=config_key)

    df = vidclub.to_df(
        source=source,
        region=region,
        from_date=from_date,
        to_date=to_date,
        if_empty=if_empty
    )

    logger.logger.info("Successfully downloaded data to a DataFrame.")
    return df
