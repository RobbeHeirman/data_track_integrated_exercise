import argparse
import json
import logging
import os
import sys

import boto3

import rest_interface
from utils import log_function

BASE_DIR = "."


def write_json_to_s3(s3_bucket: str, filename: str, json_data: dict) -> None:
    s3 = boto3.resource('s3')
    s3_object = s3.Object(s3_bucket, filename)
    s3_object.put(Body=(bytes(json.dumps(json_data).encode('UTF-8'))))


def write_json_to_file(subdir: str, filename: str, json_data: dict) -> None:
    nw_dir = os.path.join(BASE_DIR, subdir)
    os.makedirs(nw_dir, exist_ok=True)
    full_filename = os.path.join(nw_dir, filename)
    with open(full_filename, "w+") as f:
        f.write(json.dumps(json_data, indent=2))


@log_function
def handle_stations(s3_bucket: str, iso_date: str) -> None:
    stations_endpoint = rest_interface.RestInterface("stations?expanded=true")
    stations = stations_endpoint.get_list()
    for station in stations:
        station_id = station["properties"]["id"]
        filename = f"robbe-data/{iso_date}/stations/station_{station_id}.json"
        write_json_to_s3(s3_bucket, filename, station)


@log_function
def handle_timeseries(s3_bucket: str, iso_date: str) -> None:
    """"
    Require date in iso_date
    """
    timespan = f"PT24H/{iso_date}"
    timeseries = rest_interface.RestInterface(f"timeseries?timespan={timespan}").get_list()
    timeserie_data = rest_interface.TimeSeriesRestInterface("timeseries").get_timeseries_data_of_keys(timeseries,
                                                                                                      timespan)
    for timeserie in timeseries:
        timeserie_id = timeserie['id']
        if not (this_timeserie := timeserie_data.get(timeserie_id, None)):
            continue

        timeserie['values'] = this_timeserie["values"]
        filename = f"robbe-data/{iso_date}/timeseries/timeseries_{timeserie_id}.json"
        write_json_to_s3(s3_bucket, filename, timeserie)


def process_raw_data(s3_bucket: str, date: str) -> None:
    handle_stations(s3_bucket, date)
    handle_timeseries(s3_bucket, date)


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
    print(args.env)
    process_raw_data(args.env, args.date)


if __name__ == "__main__":
    main()
