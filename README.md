# Google BigQuery Plugin

Sends events to a BigQuery database on ingestion.

## Installation

1. Visit 'Plugins' in PostHog
1. Find this plugin from the repository or install `https://github.com/PostHog/bigquery-plugin`
1. Configure the plugin
   1. Upload your Google Cloud key `.json` file. (See below for permissions and how to retrieve this.)
   1. Enter your Dataset ID
   1. Enter your Table ID 
1. Watch events roll into BigQuery

## Service Account, Table Setup & Permissions

To setup permissions for the BigQuery plugin, you'll need:
1. A service account 
2. A dataset which has permissions for the Service Account to use.

Here's how to set these up such that the PostHog plugin has limited access to only the table it needs.

First [create a service account](https://cloud.google.com/bigquery/docs/reference/libraries#setting_up_authentication). Keep hold of the JSON file at the end of these steps for setting up the plugin, and remember the name too.

You can create a role which has only the specific permissions the PostHog BigQuery plugin requires (listed below), or use the built in `BigQuery DataOwner` permission. If you create a custom role, you will need:
* bigquery.datasets.get
* bigquery.tables.create
* bigquery.tables.get
* bigquery.tables.list
* bigquery.tables.updateData

Next, create a dataset within a BigQuery Project (ours is called `posthog` but any name will do).

Follow the instructions [on granting access to a dataset in BigQuery](https://cloud.google.com/bigquery/docs/dataset-access-controls#granting_access_to_a_dataset) to ensure your new service account has been granted either the role you created or the "BigQuery Data Owner" permission. 

It's here:
<img width="1417" alt="SQL_workspace_–_BigQuery_–_Data_Warehouse_Exp_–_Google_Cloud_Platform" src="https://user-images.githubusercontent.com/1108173/130323561-444cbbf6-a994-455e-97b6-8db6df69e274.png">

Use the Share Dataset button to share your dataset with your new service account and either the `BigQuery DataOwner` role, or your custom role created above. In the below, we've used a custom role `PostHog Ingest`.

<img width="480" alt="SQL_workspace_–_BigQuery_–_Data_Warehouse_Exp_–_Google_Cloud_Platform" src="https://user-images.githubusercontent.com/1108173/130323602-50f13200-6fde-4ee9-b507-1bce75fc75b2.png">

That's it! Once you've done the steps above, your data should start flowing from PostHog to BigQuery.

## Troubleshooting

### Duplicate Events

There's a very rare case when duplicate events appear in Bigquery. This happens due to network errors, where the export seems to have failed, yet it actually reaches Bigquery.

While this shouldn't happen, if you find duplicate events in Bigquery, follow these [official docs on Bigquery](https://cloud.google.com/bigquery/streaming-data-into-bigquery#manually_removing_duplicates) to manually remove the duplicates.
