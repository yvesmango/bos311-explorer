# Executive Summary

**Exploratory Questions**  
- How effectively is Boston responding to residents' 311 service requests? 
- Where do delays or geographic inequities appear in municipal service delivery?

**What We Measured**  
We tracked the full lifecycle of municipal service requests, from initial report to closure. The core metrics include SLA compliance, response time, ticket volume, and the geographic distribution of open versus resolved requests across neighborhoods, wards, and city council districts.

**Data Sources**  
This project ingests live and historical Boston 311 data from the City of Boston open data portal and normalizes it into a clean warehouse for analysis.

**Key Findings**  
Boston processes a large volume of service requests, but the workload is not evenly distributed. Some service categories resolve quickly while others lag behind, and some neighborhoods experience more persistent delays than others. Those patterns are exactly the kind of civic signal a newsroom or public-interest analyst can use to identify accountability stories.

**Methodology Note**  
A major challenge in Boston 311 is the transition from a legacy vendor to a new vendor. The data does not arrive in one stable format. This project solves that problem with a normalization layer that maps both systems into a single schema, preserves raw payloads for auditability, and tags every record with its source system. That keeps the historical record consistent even as the city’s backend changes.

**Why It Matters**  
This repository is designed to support story finding. It gives editors, reporters, and civic readers a structured way to ask questions like:

- Where are overdue tickets clustering?
- Which service categories are slowest to resolve?
- Are certain districts getting faster or slower responses over time?

Instead of relying on anecdotes, the project provides a clean data foundation for public accountability.

**Limitations**  
The analysis reflects reported service requests, not every underlying condition in the city. Reporting patterns can be influenced by neighborhood internet access, language access, and awareness of the 311 system. The project also depends on the city’s own source data, so coverage will continue to evolve as the vendor transition progresses.
