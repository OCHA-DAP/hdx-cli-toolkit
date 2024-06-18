#!/bin/bash

hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-all-ongoing-emergency-responses-may-2021-update
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-all-emergency-responses-28-feb-2018-update
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-echo-funded-emergency-responses-30-june-2020-update
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-echo-funded-erm-vii-ongoing-emergency-responses-30-april-2019-update-by-sectors-ips
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-echo-funded-emergency-responses-30-june-2020-update-and-hotspot-woredas
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=rc-ethiopia-ofda-funded-rrm-emergency-responses-30-june-2020-update
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-ofda-echo-funded-emergency-responses-31-august-2018-update-by-sectors-ips
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=sharpe-project-intervention-woredas-refugee-camps-versus-livelihoods_gambella-region
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-all-ongoing-emergency-responses-aug-2021-update-by-sectors-donors-ips
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-echo-funded-emergency-responses-31july-2020-update
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-proposed-kebeles-charity-water-project_sidama-region
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=-ethiopia-ofda-funded-emergency-responses-31-july-2020-update
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-ofda-funded-rrm-emergency-responses-31-july-2020-update

hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-ofda-funded-rrm-emergency-responses-30-june-2020-update
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=irc-ethiopia-ofda-funded-emergency-responses-31-july-2020-update
hdx-toolkit remove_extras_key --hdx_site=$1 --dataset_name=hotspot-woredas-january-2020

hdx-toolkit update --organization=irc-ethiopia --dataset_filter=* --hdx_site=$1 --key=archived --value=True
hdx-toolkit update --organization=irc-ethiopia --dataset_filter=* --hdx_site=$1 --key=data_update_frequency --value=-1
hdx-toolkit update --organization=irc-ethiopia --dataset_filter=* --hdx_site=$1 --key=maintainer --value=e32d5afb-cc7e-4715-85b7-5a3b849443f5