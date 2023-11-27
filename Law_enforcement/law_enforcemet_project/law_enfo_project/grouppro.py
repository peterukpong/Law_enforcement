from typing import Dict, Any, Optional, Sequence, Union
from airflow.models import BaseOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import pandas as pd
import requests

class WebToGCSHKOperator(BaseOperator):
    def __init__(
        self,
        gcs_bucket_name: str,
        gcs_object_name: str,
        api_endpoint: str,
        api_headers: Dict[str, str],
        api_params: Dict[str, Union[str, int]],
        gcp_conn_id: str,  # Add the parameter here
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_object_name = gcs_object_name
        self.api_endpoint = api_endpoint
        self.api_headers = api_headers
        self.api_params = api_params
        self.gcp_conn_id = gcp_conn_id  # Store the parameter

    def execute(self, context: Dict[str, Any]) -> None:
        # Make an authenticated GET request to the API
        response = requests.get(self.api_endpoint, params=self.api_params, headers=self.api_headers)

        if response.status_code == 200:
            # Convert the response JSON to a DataFrame
            results_df = pd.DataFrame(response.json())

            # Save the DataFrame to a CSV file
            csv_content = results_df.to_csv(index=False)

            # Upload the CSV content to GCS
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)  # Use the parameter
            gcs_hook.upload(
                bucket_name=self.gcs_bucket_name,
                object_name=self.gcs_object_name,
                data=csv_content.encode('utf-8'),
                mime_type='text/csv',
            )

            self.log.info(f"Data uploaded to GCS: gs://{self.gcs_bucket_name}/{self.gcs_object_name}")
        else:
            self.log.error(f"Failed to retrieve data. Status code: {response.status_code}")
            raise ValueError(f"Failed to retrieve data. Status code: {response.status_code}")
