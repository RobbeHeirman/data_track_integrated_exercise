import argparse
import logging
import sys
import os
import rest_interface
import json

BASE_DIR = "."


def write_json_to_s3(s3_bucket, key: str):
    pass


def write_json_to_file(subdir: str, filename: str, json_data: dict) -> None:
    nw_dir = os.path.join(BASE_DIR, subdir)
    os.makedirs(os.path.dirname(nw_dir), exist_ok=True)
    full_filename = os.path.join(nw_dir, filename)
    with open(full_filename, "w+") as f:
        f.write(json.dumps(json_data, indent=2))


def process_raw_data(s3_bucket: str, date: str):
    # TODO handle query params gracefully
    stations_endpoint = rest_interface.RestInterface("stations?expanded=true")
    stations = stations_endpoint.get_list()
    write_json_to_file("stations", "first", stations[0])
    for station in stations:
        station_id = station["properties"]["id"]
        filename = f"station_{station_id}.json"
        write_json_to_file("stations", filename, station)


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
    process_raw_data("", "")
    # main()
