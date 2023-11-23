import argparse
import json
import logging
import os
import sys

import boto3

import rest_interface

BASE_DIR = "."


def write_json_to_s3(s3_bucket: str, filename: str, json_data: dict):
    s3 = boto3.resource('s3')
    b_name = 'data-track-integrated-exercise'
    s3_object = s3.Object(b_name, filename)
    s3_object.put(Body=(bytes(json.dumps(json_data).encode('UTF-8'))))


def write_json_to_file(subdir: str, filename: str, json_data: dict) -> None:
    nw_dir = os.path.join(BASE_DIR, subdir)
    os.makedirs(nw_dir, exist_ok=True)
    full_filename = os.path.join(nw_dir, filename)
    with open(full_filename, "w+") as f:
        f.write(json.dumps(json_data, indent=2))


def handle_stations(iso_date: str):
    print("start fetch stations")
    stations_endpoint = rest_interface.RestInterface("stations?expanded=true")
    stations = stations_endpoint.get_list()
    print("end fetch stations")
    print("start write stations")
    for station in stations:
        station_id = station["properties"]["id"]
        filename = f"robbe-data/{iso_date}/stations/station_{station_id}.json"
        write_json_to_s3("", filename, station)
    print("end fetch stations")


def handle_timeseries(iso_date: str):
    """"
    Require date in iso_date
    """

    print("start fetch timeseries keys")
    timespan = f"PT24H/{iso_date}"
    timeseries = rest_interface.RestInterface(f"timeseries?timespan={timespan}").get_list()
    print("end fetch timeseries keys")
    print("start fetch timeseries data")
    timeserie_data = rest_interface.TimeSeriesRestInterface("timeseries").get_timeseries_data_of_keys(timeseries,
                                                                                                      timespan)
    print("end fetch time series data")
    print("start write time series")
    for timeserie in timeseries:
        timeserie_id = timeserie['id']
        timeserie['values'] = timeserie_data[timeserie_id]["values"]
        filename = f"robbe-data/{iso_date}/timeseries/timeseries_{timeserie_id}.json"
        write_json_to_s3("", filename, timeserie)
    print("end write time series")


def process_raw_data(s3_bucket: str, date: str):
    # TODO handle query params gracefully
    handle_stations(date)
    handle_timeseries(date)


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    parser = argparse.ArgumentParser(description="Building greeter")
    parser.add_argument(
        "-d", "--date", dest="date", help="date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-e", "--env", dest="env", help="The environment in which we execute the code", required=True
    )
    args = parser.parse_args()
    logging.info(f"Using args: {args}")
    # ingest_data(args.path, args.date)


if __name__ == "__main__":
    # TODO : fix this
    # process_raw_data("", "")
    process_raw_data("", "2023-11-22")
    # main()
