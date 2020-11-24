# “Copyright 2019 Google LLC. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreements with Google.”  

from google.cloud import datacatalog_v1
from google.cloud.datacatalog_v1.types import Tag
from google.cloud.datacatalog_v1.types import TagTemplate
from google.cloud.datacatalog_v1.types import TagTemplateField
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, PermissionDenied
import argparse

parser = argparse.ArgumentParser(description='Sample program to populate BQ table tags in Data Catalog.')
parser.add_argument('--project', help='GCP Project ID', required=True)
parser.add_argument('--region', help='GCP Region for the Data Catalog Tag Template (e.g., us-central1)', required=True)
parser.add_argument('--template', help='Data Catalog Tag Template Name', required=True)
parser.add_argument('--dataset', help='BigQuery Dataset Name', required=True)
parser.add_argument('--table', help='BigQuery Table Name', required=True)

args = parser.parse_args()
project_id = args.project
region = args.region
template_id = args.template
dataset_id = args.dataset
table_id = args.table


# export GOOGLE_APPLICATION_CREDENTIALS="path_to_service_account_credential"
# Service Account Roles:
#  BigQuery Data Viewer
#  BigQuery Job User
#  Data Catalog Tag Editor
#  Data Catalog TagTemplate Creator 
dc_client = datacatalog_v1.DataCatalogClient()
template_path = dc_client.tag_template_path(project_id, region, template_id)
print("Template Path: {template_path}".format(template_path=template_path))

# Check if tag template already exists, if not, create it
template = None
try:
    template = dc_client.get_tag_template(template_path)
except (NotFound, PermissionDenied):
    print("Template {template_id} not found. Creating...".format(template_id=template_id))
    template = TagTemplate()
    template.display_name = "Test Template Generated from Python Client"
    template.fields["count"].display_name = 'Count'
    template.fields["count"].is_required = False
    template.fields["count"].type.primitive_type = datacatalog_v1.enums.FieldType.PrimitiveType.DOUBLE.value

    template.fields["last_modified"].display_name = 'Last Modified'
    template.fields["last_modified"].is_required = False
    template.fields["last_modified"].type.primitive_type = datacatalog_v1.enums.FieldType.PrimitiveType.TIMESTAMP.value

    parent = dc_client.location_path(project_id, region)
    template = dc_client.create_tag_template(parent=parent, tag_template_id=template_id, tag_template=template)

# Execute queries to extract information for populating data catalog tags
# Row Count
table_full_name = "{project_id}.{dataset_id}.{table_id}".format(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id)
print("Counting rows for {table_full_name} ...".format(table_full_name=table_full_name))
bq_client = bigquery.Client()
query_str = "select count(1) from `{table_full_name}`;".format(table_full_name=table_full_name)
rows = bq_client.query(query_str).result()
count_result = None
for row in rows: 
    #print("query result: " + str(row[0]))
    count_result = row[0]

# Last Modified
print("Extracting last modified date for {table_full_name} ...".format(table_full_name=table_full_name))
query_str = "select TIMESTAMP_MILLIS(last_modified_time) from `{project_id}.{dataset_id}.__TABLES__` where table_id = '{table_id}';".format(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id)
rows = bq_client.query(query_str).result()
last_modified_result = None
for row in rows:
    last_modified_result = row[0]

# Get the tag entry object for the BQ table
print("Looking up Data Catalog entry for {table_full_name} ...".format(table_full_name=table_full_name))
bq_table_resource = "//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}".format(
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table_id)
entry = dc_client.lookup_entry(linked_resource=bq_table_resource)

# Check if tags already exist for the BQ table on the tag template
tag_exist = False
tag_id = None
existing_tags = dc_client.list_tags(parent=entry.name)
for tag_instance in existing_tags:
    if tag_instance.template == template_path:
        tag_exist = True
        tag_id = tag_instance.name

# Populate tag values
tag = Tag()
tag.template = template_path
tag.fields["count"].double_value = count_result
tag.fields["last_modified"].timestamp_value.FromDatetime(last_modified_result)

# Create or update tag values
if tag_exist:
    print("Tags already exist for {table_full_name} on template {template_id}. Updating tags with new values...".format(
        table_full_name=table_full_name, 
        template_id=template_id))
    tag.name = tag_id
    dc_client.update_tag(tag=tag)
else:
    print("Creating tags...")
    dc_client.create_tag(parent=entry.name, tag=tag)

print("Completed.")
