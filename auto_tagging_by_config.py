# “Copyright 2019 Google LLC. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreements with Google.”  

from google.cloud import datacatalog_v1
from google.cloud.datacatalog_v1.types import Tag
from google.cloud.datacatalog_v1.types import TagTemplate
from google.cloud.datacatalog_v1.types import TagTemplateField
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, PermissionDenied
import argparse
import yaml
import re

parser = argparse.ArgumentParser(description='Sample program to populate BQ table tags in Data Catalog.')
parser.add_argument('--tag_config', help='Tag Config YAML file', required=True)

args = parser.parse_args()
tag_config_file = args.tag_config

# Read tag config from YAML
tag_config = {}
with open('tag_config.yaml') as f:
    tag_config = yaml.load(f, Loader=yaml.FullLoader)

project_id = tag_config["tag_config"]["template"]["project_id"]
region = tag_config["tag_config"]["template"]["region"]
template_id = tag_config["tag_config"]["template"]["template_id"]
fields = tag_config["tag_config"]["fields"]
rules = tag_config["tag_config"]["rules"]

bq_resources = {}
for rule in rules:
    uri = rule.split("/")
    project = uri[2]
    dataset = uri[4]
    table_pattern = uri[5].replace("*", ".*") # convert wildcard to regex .*
    if project not in bq_resources:
        bq_resources[project] = {}
    if dataset not in bq_resources[project]:
        bq_resources[project][dataset] = []
    bq_resources[project][dataset].append(table_pattern)


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
    template.display_name = "Auto Template Population Demo"
    template.fields["data_owner"].display_name = 'Data Owner'
    template.fields["data_owner"].is_required = False
    template.fields["data_owner"].type.primitive_type = datacatalog_v1.enums.FieldType.PrimitiveType.STRING.value

    template.fields["source"].display_name = 'Source'
    template.fields["source"].is_required = False
    template.fields["source"].type.primitive_type = datacatalog_v1.enums.FieldType.PrimitiveType.STRING.value

    parent = dc_client.location_path(project_id, region)
    template = dc_client.create_tag_template(parent=parent, tag_template_id=template_id, tag_template=template)

# Enumerate all datasets.tables and populate tags
for project in bq_resources:
    bq_client = bigquery.Client(project=project)
    datasets = list(bq_client.list_datasets())
    for dataset in datasets:
        dataset_id = dataset.dataset_id
        if dataset_id in bq_resources[project]:
            tables = list(bq_client.list_tables(dataset_id))
            for table in tables:
                table_id = table.table_id
                
                table_id_match_pattern = False
                for table_pattern in bq_resources[project][dataset_id]:
                    if re.match(table_pattern, table_id):
                        table_id_match_pattern = True

                if table_id_match_pattern:
                    table_full_name = "{project_id}.{dataset_id}.{table_id}".format(
                                        project_id=project_id,
                                        dataset_id=dataset_id,
                                        table_id=table_id)
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

                    for field in fields:
                        tag.fields[field["name"]].string_value = field["value"]

                    # Create or update tag values
                    if tag_exist:
                        print("Tags already exist for {table_full_name} on template {template_id}. Updating tags with new values...".format(
                            table_full_name=table_full_name, 
                            template_id=template_id))
                        tag.name = tag_id
                        dc_client.update_tag(tag=tag)
                    else:
                        print("Creating tags for {table_full_name} on template {template_id}...".format(
                            table_full_name=table_full_name,
                            template_id=template_id))
                        dc_client.create_tag(parent=entry.name, tag=tag)

print("Completed.")
