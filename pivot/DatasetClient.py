import json

import requests

from pivot.Exceptions import DatasetException


class DatasetClient:
    def __init__(self, base_url: str, access_token: str, dataset_id: str, check_conn:bool = False):
        self.base_url = base_url
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Token {self.access_token}",
            "Content-Type": "application/json",
        }

        self.dataset_id = dataset_id

        # validate that dataset exists

        if check_conn:
            if (
                    requests.request(
                        "GET",
                        f"{self.base_url}/datasets/{self.dataset_id}",
                        headers=self.headers,
                    ).status_code
                    == 404
            ):
                raise DatasetException(
                    "The dataset you tried to access does not exist\nPlease first create this dataset."
                )

    def query(
            self, query=None, sort=None, limit: int = 25, skip: int = 0, simple: bool = True
    ) -> dict:
        if sort is None:
            sort = {}
        if query is None:
            query = {}

        res = requests.request(
            "POST",
            f"{self.base_url}/datasets/{self.dataset_id}/query",
            data=json.dumps(
                {"query": query, "sort": sort, "limit": limit, "skip": skip}
            ),
            headers=self.headers,
        ).json()

        if simple:
            return res["data"]
        return res

    def get(self, query=None) -> dict:

        if query is None:
            query = {}

        try:
            return self.query(query)[0]
        except:
            raise DatasetException("Instance does not exist")

    def write(self, objects: list, simple: bool = True) -> dict:
        res = requests.request(
            "POST",
            f"{self.base_url}/datasets/{self.dataset_id}/write",
            data=json.dumps({"objects": objects}),
            headers=self.headers,
        ).json()
        if simple:
            return res["data"]
        return res

    def aggregate(self, pipeline: list, simple: bool = True) -> dict:
        res = requests.request(
            "POST",
            f"{self.base_url}/datasets/{self.dataset_id}/aggregate",
            data=json.dumps({"pipeline": pipeline}),
            headers=self.headers,
        ).json()
        if simple:
            return res["data"]
        return res

    def update(self, query: dict, update: dict, simple: bool = True) -> dict:
        res = requests.request(
            "PUT",
            f"{self.base_url}/datasets/{self.dataset_id}/update",
            data=json.dumps({"query": query, "update": update}),
            headers=self.headers,
        ).json()

        if simple:
            return res["data"]
        return res

    def delete(self, query=None) -> dict:
        if query is None:
            query = {}
        return requests.request(
            "DELETE",
            f"{self.base_url}/datasets/{self.dataset_id}/delete",
            data=json.dumps(
                {
                    "query": query,
                }
            ),
            headers=self.headers,
        ).json()
