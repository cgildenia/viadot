import pytest
import pandas as pd

from viadot.sources import Vidclub

@pytest.fixture(scope='function')
def vidclub():
    vidclub = Vidclub(config_key="vidclub")
    yield vidclub

# COLS_EXPECTED_JOBS = []

# COLS_EXPECTED_PRODUCT = []

COLS_EXPECTED_SURVEY = ['id','type','text','_viadot_source', '_viadot_downloaded_at_utc']

def test_response_jobs(vidclub):
    df = vidclub.to_df(
            source= "jobs",
            from_date= "2023-01-01",
            to_date= "2023-01-02",
            limit= 20
        )
    
    assert len(list(df.columns)) == 25

def test_response_product(vidclub):
    df = vidclub.to_df(
            source= "product",
            from_date= "2023-01-01",
            to_date= "2023-01-02",
            limit= 20
        )
    
    assert len(list(df.columns)) == 10


def test_response_survey(vidclub):
    df = vidclub.to_df(
            source= "survey",
            from_date= "2023-01-01",
            to_date= "2023-01-02",
            limit= 20
        )
    
    assert list(df.columns) == COLS_EXPECTED_SURVEY