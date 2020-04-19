# Coronavirus Dashboard

## Consumer data pipeline ETL

ETL service to create consumer-ready CSV and JSON files for download.

The service is dispatched by an event that is triggered every time
a new data file is deployed to the ``publicdata`` blob storage.

Data are identical to the original source, but enjoys a different structure.

> **Note:** There are missing values in the data. The approach is to leave them
  as blank in the CSV file, and assign a ``null`` value in JSON to
  ensure a consistent structure.

### Credits
This service is developed and maintained by [Public Health England](https://www.gov.uk/government/organisations/public-health-england).

---

Copyright (c) 2020 Public Health England