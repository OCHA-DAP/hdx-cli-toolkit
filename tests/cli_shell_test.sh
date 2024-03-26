#!/bin/bash
echo "Executing read only hdx-toolkit commands"
hdx-toolkit --help
hdx-toolkit list --help
hdx-toolkit configuration
hdx-toolkit list --organization=healthsites --dataset_filter=*al*-healthsites --hdx_site=stage --key=private --value=True
hdx-toolkit list --organization=international-organization-for-migration --key=data_update_frequency,dataset_date --output_path=list-test-1.csv
rm list-test-1.csv
hdx-toolkit list --query=archived:true --key=owner_org --output_path=list-test-2.csv
rm list-test-2.csv
hdx-toolkit get_organization_metadata --organization=zurich
hdx-toolkit get_organization_metadata --organization=eth-zurich-weather-and-climate-risks --verbose
hdx-toolkit get_user_metadata --user=hopkinson
hdx-toolkit get_user_metadata --user=hopkinson --verbose
hdx-toolkit print --dataset_filter=climada-litpop-dataset
hdx-toolkit print --dataset_filter=wfp-food-prices-for-nigeria --with_extras
echo "Reached end successfully"