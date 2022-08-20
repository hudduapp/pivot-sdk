import requests

from pivot.DatasetClient import DatasetClient
from pivot.Exceptions import DatasetException


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

    def get(self, dataset_id: str = None, dataset_name: str = None) -> DatasetClient:

        if dataset_id:
            return DatasetClient(self.base_url, self.access_token, dataset_id=dataset_id)
        elif dataset_name:
            try:
                dataset_id = self.list_datasets(f"name:{dataset_name}")["data"][0]["id"]
            except:
                raise DatasetException("no dataset with dataset_name found")
            return DatasetClient(self.base_url, self.access_token, dataset_id=dataset_id)
        else:
            raise DatasetException("one of dataset_id or dataset_name have to be specified.")
