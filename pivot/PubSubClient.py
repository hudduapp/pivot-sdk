import json
from typing import List

import requests

from pivot.Exceptions import DatasetException


class PubSubClient:
    def __init__(self, base_url: str, access_token: str, topic_id: str):
        self.base_url = base_url
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Token {self.access_token}",
            "Content-Type": "application/json",
        }

        self.topic_id = topic_id

        # validate that dataset exists

        if (
                requests.request(
                    "GET", f"{self.base_url}/topics/{self.topic_id}", headers=self.headers
                ).status_code
                == 404
        ):
            raise DatasetException(
                "The dataset you tried to access does not exist\nPlease first create this dataset."
            )

    def push(self, objects: list) -> dict:
        res = requests.request(
            "POST",
            f"{self.base_url}/datasets/{self.topic_id}/push",
            data=json.dumps({"objects": objects}),
            headers=self.headers,
        ).json()

        if simple:
            return res["data"]
        return res

    def pull(self, limit: int = 25, reserve: bool = False) -> dict:
        res = requests.request(
            "POST",
            f"{self.base_url}/topics/{self.topic_id}/pull",
            data=json.dumps({"reserve": reserve,
                             "limit": limit}),
            headers=self.headers,
        ).json()

        if simple:
            return res["data"]
        return res

    def acknowledge(self, ids: List[str]) -> dict:
        res = requests.request(
            "POST",
            f"{self.base_url}/topics/{self.topic_id}/acknowledge",
            data=json.dumps({"ids": ids}),
            headers=self.headers,
        ).json()

        if simple:
            return res["data"]
        return res
