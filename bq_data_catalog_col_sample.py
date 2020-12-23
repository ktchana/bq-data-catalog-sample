# “Copyright 2019 Google LLC. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreements with Google.”  

from google.cloud import datacatalog_v1
from google.cloud.datacatalog_v1.types import Tag
from google.cloud.datacatalog_v1.types import TagTemplate
from google.cloud.datacatalog_v1.types import TagTemplateField
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, PermissionDenied
import argparse

parser = argparse.ArgumentParser(description='Sample program to populate BQ column tags in Data Catalog.')
parser.add_argument('--project', help='GCP Project ID', required=True)
parser.add_argument('--region', help='GCP Region for the Data Catalog Tag Template (e.g., us-central1)', required=True)
parser.add_argument('--template', help='Data Catalog Tag Template Name', required=True)
parser.add_argument('--dataset', help='BigQuery Dataset Name', required=True)
parser.add_argument('--table', help='BigQuery Table Name', required=True)
parser.add_argument('--column', help='BigQuery Table Column Name', required=True)

args = parser.parse_args()
project_id = args.project
region = args.region
template_id = args.template
dataset_id = args.dataset
table_id = args.table
column = args.column


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
    template.display_name = "column data quality"
    
    template.fields["null_count"].display_name = 'Null Count'
    template.fields["null_count"].is_required = False
    template.fields["null_count"].type.primitive_type = datacatalog_v1.enums.FieldType.PrimitiveType.DOUBLE.value
    
    template.fields["unique_values"].display_name = 'Unique Values'
    template.fields["unique_values"].is_required = False
    template.fields["unique_values"].type.primitive_type = datacatalog_v1.enums.FieldType.PrimitiveType.DOUBLE.value
    
    template.fields["perc_missing"].display_name = 'Percentage missing'
    template.fields["perc_missing"].is_required = False
    template.fields["perc_missing"].type.primitive_type = datacatalog_v1.enums.FieldType.PrimitiveType.DOUBLE.value

    parent = dc_client.location_path(project_id, region)
    template = dc_client.create_tag_template(parent=parent, tag_template_id=template_id, tag_template=template)

# Execute queries to extract information for populating data catalog tags
# count null, unique values and percent missing
bq_client = bigquery.Client()
table_full_name = "{project_id}.{dataset_id}.{table_id}".format(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id)
print("Calculating statistics for {table_full_name} ...".format(table_full_name=table_full_name))
query_str = """WITH
                  base_table AS (
                      SELECT
                        {column}
                      FROM
                        {table_full_name}),
                  t2 AS (
                      SELECT
                        COUNT(DISTINCT {column}) AS distinct_count
                      FROM
                        base_table),
                  t3 AS (
                      SELECT
                        COUNT(1) AS all_count
                      FROM
                        base_table ),
                  t4 AS (
                      SELECT
                        COUNT(1) AS null_count
                      FROM
                        base_table
                      WHERE
                        {column} IS NULL)
                SELECT
                  t2.distinct_count,
                  t4.null_count,
                  t4.null_count/t3.all_count AS null_percentage
                FROM
                  t2,
                  t3,
                  t4 ;""".format(
                                table_full_name=table_full_name,
                                column=column)
rows = bq_client.query(query_str).result()
unique_values_result = None
null_result = None
perc_missing_result = None
for row in rows:
    unique_values_result = row[0]
    null_result = row[1]
    perc_missing_result = row[2]

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
        if tag_instance.column == column:
            tag_exist = True
            tag_id = tag_instance.name

# Populate tag values
tag = Tag()
tag.template = template_path
tag.fields["null_count"].double_value = null_result
tag.fields["unique_values"].double_value = unique_values_result
tag.fields["perc_missing"].double_value = perc_missing_result
tag.column = column

# Create or update tag values
if tag_exist:
    print("Tags already exist for {table_full_name}.{column} on template {template_id}. Updating tags with new values...".format(
        table_full_name=table_full_name, 
        template_id=template_id,
        column=column))
    tag.name = tag_id
    dc_client.update_tag(tag=tag)
else:
    print("Creating tags...")
    dc_client.create_tag(parent=entry.name, tag=tag)

print("Completed.")
