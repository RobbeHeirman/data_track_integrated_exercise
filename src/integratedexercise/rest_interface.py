import requests

BASE_URL = "https://geo.irceline.be/sos/api/v1/"


class RestInterface:

    def __init__(self, endpoint):
        self._endpoint_url = f"{BASE_URL}{endpoint}"

    def get_list(self):
        # TODO: Error handling
        return requests.get(self._endpoint_url).json()
