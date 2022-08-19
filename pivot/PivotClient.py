import requests

from pivot.DatasetClient import DatasetClient


class PivotClient:
    def __init__(self, base_url: str, access_token: str):
        self.base_url = base_url
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Token {self.access_token}",
            "Content-Type": "application/json"
        }

    def create_dataset(self, name: str, database: str) -> dict:
        return requests.request("POST", f"{self.base_url}/datasets", data={
            "name": name,
            "database": database
        }, headers=self.headers).json()

    def delete_dataset(self, dataset_id: str) -> dict:
        return requests.request("DELETE", f"{self.base_url}/datasets/{dataset_id}", headers=self.headers).json()

    def retrieve_dataset(self, dataset_id: str) -> dict:
        return requests.request("GET", f"{self.base_url}/datasets/{dataset_id}", headers=self.headers).json()

    def list_datasets(self, query: str = "", sort: str = "", limit: int = 25,
                      skip: int = 0) -> dict:
        return requests.request("GET", f"{self.base_url}/datasets", params={
            "query": query,
            "sort": sort,
            "limit": limit,
            "skip": skip
        }, headers=self.headers).json()

    def get(self, dataset_id: str) -> DatasetClient:
        return DatasetClient(self.base_url, self.access_token, dataset_id=dataset_id)
