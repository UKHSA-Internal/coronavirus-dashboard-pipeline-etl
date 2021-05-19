# Coronavirus (COVID-19) in the UK - ETL Service

## Consumer data pipeline ETL

ETL service to create consumer-ready CSV and JSON files for download.

The service is dispatched by an event that is triggered every time
a new data file is deployed to the ``publicdata`` blob storage.

Data are identical to the original source, but enjoys a different structure.

> **Note:** There are missing values in the data. The approach is to leave them
  as blank in the CSV file, and assign a ``null`` value in JSON to
  ensure a consistent structure.


## Other repositories

Different parts of the Coronavirus dashboard service are maintained in their respective 
repositories, itemised as follows:
 
- [API v.1](https://github.com/publichealthengland/coronavirus-dashboard-api-v1) - Main API service for the data, lookup tables, CMS, and metadata.
- [API v.2.0](https://github.com/publichealthengland/coronavirus-dashboard-api-v2) - Batch downloads service [DEPRECATED in favour of v2.1]
- [API v.2.1](https://github.com/publichealthengland/coronavirus-dashboard-api-v2-server) - Batch downloads service
- [Generic APIs](https://github.com/publichealthengland/coronavirus-dashboard-generic-apis) - APIs that power the map, navigation, hierarchy, and relations
- [Coronavirus Dashboard](https://github.com/publichealthengland/coronavirus-dashboard) - Details pages
- [Frontend Server](https://github.com/publichealthengland/coronavirus-dashboard-frontend-server) - Home and postcode pages
- [Easy-Read Server](https://github.com/publichealthengland/coronavirus-dashboard-easy-read) - Easy-read pages and PDF generator
- [Layout CMS](https://github.com/publichealthengland/coronavirus-dashboard-layouts) - Definition of contents
- [Metadata](https://github.com/publichealthengland/coronavirus-dashboard-metadata) - Description of metrics and textual contents
- [Terraform](https://github.com/publichealthengland/coronavirus-dashboard-terraform) - Infrastructure as Code [DEPRECATED in favour of ARM templates]

### Credits
This service is developed and maintained by [Public Health England](https://www.gov.uk/government/organisations/public-health-england).

---

Copyright (c) 2020 Public Health England
