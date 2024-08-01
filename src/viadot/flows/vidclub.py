
from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional

from prefect import flow

from prefect_viadot.tasks import df_to_adls, vidclub_to_df


@flow(
name="extract--vidclub--adls",
description="Extract data from vidclub and load it into Azure Data Lake.",
retries=1,
retry_delay_seconds=60,
)
def vidclub_to_adls(
    adls_path: str,
    adls_config_key: str = None,
    adls_credentials_secret: str = None,
    overwrite: bool = False,
    vidclub_config_key: str = None,
    vidclub_credentials_secret: str = None,
    vidclub_credentials: dict = None,
    source: str = '',
    region: str = '',
    from_date: str = '',
    to_date: str = '',
):

    """

Description:
    Flow for downloading data from vidclub to Azure Data Lake.
Args:
    adls_path (str): ADLS file path .parquet 
    adls_config_key (str, optional): The key in the viadot config holding relevant
        credentials. Defaults to None.
    adls_credentials_secret (str, optional): The name of the Azure Key Vault secret
        storing the credentials. Defaults to None.
    overwrite (bool, optional): Whether to overwrite files in the lake. Defaults
        to False.
    """

df = vidclub_to_df(
    config_key=vidclub_config_key,
    credentials_secret=vidclub_credentials_secret,
    credentials=vidclub_credentials,
    source=source,
    region=region,
    from_date=from_date,
    to_date=to_date,
    )


    
adls = df_to_adls(
    df=df,
    path=adls_path,
    credentials_secret=adls_credentials_secret,
    config_key=adls_config_key,
    overwrite=overwrite,
)
