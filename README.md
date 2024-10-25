# Azure Soccer (Transfermarkt) Project w/ Azure Data Factory and Databricks (PySpark)!

![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=flat-square&logo=apachespark&logoColor=black)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![MicrosoftSQLServer](https://img.shields.io/badge/Microsoft%20SQL%20Server-CC2927?style=for-the-badge&logo=microsoft%20sql%20server&logoColor=white)

### Technologies and Services Used
- Azure Data Factory
- Databricks
- Azure Data Lake (Gen2)
- Azure Blob Storage
- Python
- PySpark
- SQL
- Azure SQL Database
- PowerBI

## Purpose and Summary
The goal of this data engineering project was to move soccer transfermarkt (German data source) through an Azure Data Factory pipeline system, ultimately it landing in a Power BI dashboard. 

## Data Source
Transfermarkt Data: https://www.kaggle.com/datasets/davidcariboo/player-scores

## Azure Setup
We will be using the following resources within Azure: Data Factory, Databricks, Azure Blob Storage (Storage Account), Azure Data Lake Gen2 (Storage Account), SQL Database, and a Key Vault.
A resource group was created to hold all of these resources.

![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/Azure%20Dashboard.PNG?raw=true)

## Project Steps

### Ingestion into Azure Blob Storage
The first step was to upload the CSV files into a Blob Storage container called 'soccer-data-dump'.

![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/abs%20container.PNG?raw=true)

### ADL2 Container Creation
Two containers were created inside of the Azure Data Lake Gen2: raw and clean. Raw will receive the CSV files from the Blob Storage, and the clean container will contain the Databricks transformed files.

![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/datalake%20container.PNG?raw=true)

### Azure Data Factory Datasets and Pipelines
Datasets were formed with ADF that were used to copy the CSV files from the Blob Storage to the Data Lake.

IMG
![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/Azure%20Dashboard.PNG?raw=true)

ADF pipelines were then used to check the Blob Storage files, and then the CSVs were copied into the Data Lake's raw container.

![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/adf%20datasets.PNG?raw=true)


![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/Pipeline%20ADF%20Layout.PNG?raw=true)

### Databricks Transformation
Databricks and PySpark were used to read in and transform the CSV files. After they were altered, they were then uploaded into the Data Lake's clean container.

![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/databricks.PNG?raw=true)


This Databricks notebook is orchestrated by Azure Data Factory.

![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/databricks%20in%20adf.PNG?raw=true)

### Azure Data Studio Playground
I created tables within Azure Data Studio and used the soccer transfermarkt data to play around with different SQL queries, in order to explore the data.

![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/SQL%20Table%20Example.PNG?raw=true)

### PowerBI Integration
The clean data was then uploaded into a Power BI PBIX file. **The dashboard is simply a rough visualization of some data points. It is not an accurate attempt at designing an excellent dashboard**

![alt text](https://github.com/airincs/Azure-Transfermarkt-Project/blob/main/Project%20Images/Dashboard.PNG?raw=true)























