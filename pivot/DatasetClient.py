import json

import requests

from pivot.Exceptions import DatasetException


class DatasetClient:
    def __init__(self, base_url: str, access_token: str, dataset_id: str):
        self.base_url = base_url
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Token {self.access_token}",
            "Content-Type": "application/json"
        }

        self.dataset_id = dataset_id

        # validate that dataset exists

        if requests.request("GET", f"{self.base_url}/datasets/{self.dataset_id}",
                            headers=self.headers).status_code == 404:
            raise DatasetException("The dataset you tried to access does not exist\nPlease first create this dataset.")

    def query(self, query: str = "", sort: str = "", limit: int = 25,
              skip: int = 0) -> dict:
        return requests.request("GET", f"{self.base_url}/datasets/{self.dataset_id}/query", params={
            "query": query,
            "sort": sort,
            "limit": limit,
            "skip": skip
        }, headers=self.headers).json()

    def write(self, objects: list) -> dict:
        return requests.request("POST", f"{self.base_url}/datasets/{self.dataset_id}/write", data=json.dumps({
            "objects": objects
        }), headers=self.headers).json()

    def update(self, query: str, update: dict) -> dict:
        return requests.request("PUT", f"{self.base_url}/datasets/{self.dataset_id}/update", data=json.dumps({
            "query": query,
            "update": update
        }), headers=self.headers).json()

    def delete(self, query: str) -> dict:
        return requests.request("DELETE", f"{self.base_url}/datasets/{self.dataset_id}/delete", data=json.dumps({
            "query": query,

        }), headers=self.headers).json()
