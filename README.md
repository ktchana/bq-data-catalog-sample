### bq_data_catalog_sample

This is a python script to demo how to use BigQuery and Data Catalog client to automate tagging of table metadata.

* The script accepts the following input parameters:
  * GCP Project ID, Tag Template Name, Tag Template Region, BQ Dataset and Table

* Create a sample tag template with the following fields:
  * count
  * last_modified

* Execute BQ SQL to retrieve the row count and last modified date of the BQ table

* Create or update the tag for the BQ table in Data Catalog


### Prerequisites

 * Python 3
 * GCP [Service Account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) with the following roles
   * BigQuery Data Viewer
   * BigQuery Job User
   * Data Catalog Tag Editor
   * Data Catalog TagTemplate Creator 

#### Setup

Create Python virtual environment and install dependencies:
```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

Setup [authentication](https://cloud.google.com/docs/authentication/getting-started) using service account key:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path_to_service_account_credential"
```


#### Running the demo script
````
> python bq_data_catalog_sample.py --help
usage: bq_data_catalog_sample.py [-h] --project PROJECT --region REGION --template TEMPLATE --dataset DATASET --table TABLE

Sample program to populate BQ table tags in Data Catalog.

optional arguments:
  -h, --help           show this help message and exit
  --project PROJECT    GCP Project ID
  --region REGION      GCP Region for the Data Catalog Tag Template (e.g., us-central1)
  --template TEMPLATE  Data Catalog Tag Template Name
  --dataset DATASET    BigQuery Dataset Name
  --table TABLE        BigQuery Table Name
````

