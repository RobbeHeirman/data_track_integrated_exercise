import requests
import json
BASE_URL = "https://geo.irceline.be/sos/api/v1/"


class RestInterface:

    def __init__(self, endpoint):
        self._endpoint_url = f"{BASE_URL}{endpoint}"

    def get_list(self):
        # TODO: Error handling
        return requests.get(self._endpoint_url).json()


class TimeSeriesRestInterface(RestInterface):

    def get_timeseries_data_of_keys(self, timeseries_jsons: [dict], timespan: str):
        url = f"{self._endpoint_url}/getData"
        headers = {'Content-Type': 'application/json'}
        params = {
            "timespan": timespan,
            "timeseries": list(map(lambda json: json['id'], timeseries_jsons))
        }
        return requests.post(url, data=json.dumps(params), headers=headers).json()
