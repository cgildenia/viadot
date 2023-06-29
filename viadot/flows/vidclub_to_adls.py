# import os
# from datetime import datetime
from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import Flow, task

from prefect.engine.signals import FAIL
from prefect.triggers import all_successful
from prefect.utilities import logging

from viadot.task_utils import add_ingestion_metadata_task, df_to_csv, adls_bulk_upload
from viadot.tasks import AzureDataLakeUpload, VidClubToDF

logger = logging.get_logger()
file_to_adls_task = AzureDataLakeUpload()


@task
def add_timestamp(df: pd.DataFrame, file: List = None, sep: str = "\t") -> None:
    """Add new column _viadot_downloaded_at_utc into each file given in the function.

    Args:
        files_names (List, optional): File names where to add the new column. Defaults to None.
        sep (str, optional): Separator type to load and to save data. Defaults to "\t".
    """

    if df.empty:
        logger.warning("Avoided adding a timestamp. Empty DataFrame as input.")
    else:
        df_updated = add_ingestion_metadata_task.run(df)
        df_updated.to_csv(file, index=False, sep=sep)


class VidClubToADLS(Flow):
    def __init__(
        self,
        name: str,
        source: Literal["jobs", "product", "company", "survey"],
        credentials: Dict[str, Any] = None,
        from_date: str = "2022-03-22",
        to_date: str = "",
        timeout: int = 3600,
        adls_file_path: str = None,
        adls_overwrite: bool = True,
        adls_sp_credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        self.source = source
        self.credentials = credentials
        self.from_date = from_date
        self.to_date = to_date
        self.timeout = timeout

        self.adls_file_path = adls_file_path
        self.adls_overwrite = adls_overwrite
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        super().__init__(*args, name, **kwargs)

    def vid_club_flow(self) -> Flow:
        to_df = VidClubToDF(timeout=self.timeout)

        file_names = to_df.bind(
            source=self.source,
            credentials=self.credentials,
            from_date=self.from_date,
            to_date=self.to_date,
            flow=self,
        )

        add_timestamp.bind(file_names, sep=self.sep, flow=self)

        adls_bulk_upload.bind(
            file_names=file_names,
            file_name_relative_path=self.file_path,
            adls_file_path=self.adls_file_path,
            adls_sp_credentials_secret=self.adls_sp_credentials_secret,
            adls_overwrite=self.adls_overwrite,
            timeout=self.timeout,
            flow=self,
        )

        add_timestamp.set_upstream(file_names, flow=self)
        adls_bulk_upload.set_upstream(file_names, flow=self)
