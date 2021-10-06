# Coronavirus Dashboard

## Consumer data pipeline ETL

ETL service. 

Additional details will be added in the future.

## ETL Diagram

![Sequence diagram of the ETL](etl_diagram/etl.png)

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
This service is developed and maintained by [UK Health Security Agency](https://www.gov.uk/government/organisations/uk-health-security-agency).

---

Copyright (c) 2020-2021 Public Health England