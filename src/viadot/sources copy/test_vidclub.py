
import pytest
import pandas as pd

from viadot.sources import Vidclub

test_definition = {
    "input_arguments":[<Put your arguments>],
    "stimulus":[<Put your values>],
    "expected_values":[<Put your values>]
}


@pytest.fixture(scope='function')
def test_<function_under_test>(Vidclub):
    # Create an instance of the class!
    vidclub = Vidclub()
    # Pick the method to test
    fcn_under_test = vidclub.function_under_test
    # Feed the Stimulus
    args = {}
    for arg, index in test_definition["input_arguments"]:
            args[arg] = test_definition["stimulus"][index]          
    res = fcn_under_test(**args)
    # Check results
    expected_res = test_definition["expected_values"]
    assert list(res) == expected_res
