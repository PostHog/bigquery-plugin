# Google BigQuery Plugin

Sends events to a BigQuery database on ingestion.

## Installation

1. Visit 'Plugins' in PostHog
1. Find this plugin from the repository or install `https://github.com/PostHog/bigquery-plugin`
1. Configure the plugin
   1. Upload your Google Cloud key `.json` file. ([How to get the file](https://cloud.google.com/bigquery/docs/reference/libraries).)
   1. Enter your Dataset ID
   1. Enter your Table ID 
1. Watch events roll into BigQuery

## Troubleshooting

### Duplicate Events

There's a very rare case when duplicate events appear in Bigquery. This happens due to network errors, where the export seems to have failed, yet it actually reaches Bigquery.

While this shouldn't happen, if you find duplicate events in Bigquery, follow these [official docs on Bigquery](https://cloud.google.com/bigquery/streaming-data-into-bigquery#manually_removing_duplicates) to manually remove the duplicates.