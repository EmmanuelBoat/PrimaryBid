import pytest
import pandas as pd
from solution.application_etl import ApplicationLifecycle


# Test for returning dataframe from reading .csv files
def test_read_csv_files():
    in_file = "/PrimaryBid/Part1/test/input/input_read_csv_file.csv"
    out_file = "/PrimaryBid/Part1/test/output/output_read_csv_file.csv"
    output = ApplicationLifecycle().read_csv_files(in_file)
    expected_output = pd.read_csv(out_file)

    pd.testing.assert_frame_equal(output, expected_output)


# Test for transforming application lifecycle data
def test_transform_app_data():
    out_file = "/PrimaryBid/Part1/test/input/CC Application Lifecycle.csv"
    expected_file = "/PrimaryBid/Part1/test/output/Application Lifecycle Output.csv"

    output_df = ApplicationLifecycle(app_filepath=out_file).transform_app_data()
    expected_output = pd.read_csv(expected_file)

    pd.testing.assert_frame_equal(output_df, expected_output)
