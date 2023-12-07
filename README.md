# ETL
An ETL system, written as 2 parts; a GCP service and a GCP cloud function.

The service provides access to data stored in an analytics engine (in this case Google Analytics).

The cloud function takes the data returned by the service and outputs it to another location, in this case a Google Sheets document.
