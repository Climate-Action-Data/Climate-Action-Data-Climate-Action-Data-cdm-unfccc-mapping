# ETL Script for Uploading UNFCCC CDM Registry Data to Climate Action Data Trust (CAD Trust) Chia Blockchain Database

This repository contains an ETL script designed to upload data from the UNFCCC CDM Registry to the Climate Action Data Trust (CAD Trust) Chia blockchain database. The uploading process is facilitated using the [Observer API](https://observer.climateactiondata.org/v1).

The data was published live during **UNFCCC COP 28** in Dubai, showcasing the integration of climate-related registry data with blockchain technology.

## Data Source

The data used in this ETL process was retrieved from the [UNFCCC CDM Registry](https://cdm.unfccc.int/Registry/index.html). The data is then processed and mapped to the CAD Trust standardized data model.

## Accessing the Data

The processed data can be accessed through the CAD Trust data dashboard website: [https://data.climateactiondata.org](https://data.climateactiondata.org).

## Script

The script used for this ETL process can be found in the `notebook.ipynb` file. 

## Prerequisites

Please note that this code will not work unless there is access to the **Chia Blockchain Data Layer**. This requires:

- A remote hosting service
- A Chia Digital Wallet

Ensure that you have these prerequisites set up before running the script.

---

For further inquiries, feel free to reach out.
