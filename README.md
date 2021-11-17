# Unmind-Assginment


## Overview
This pipeline build for analysing user requests. The bellow high level design describe how the data move 
from source to data warehouse.

![](PiplineDiagram.png)

| Step | Description |
|------|-------------|
| 1    | Read raw data from Landing bucket |
|1.1|   The data set included sensitive data (email).This step convert the email into hash value |
| 2 | Write data into processing bucket. before write spark job do validation (datatype) and add standardization |
|3 | Read from processing .And transform data according to DW table format. |
| 4 | Write data into Staging bucket |
| 5 | Read data from Staging bucket this step we can avoid spark job and can use data base bulk load option. |
|5.5 | get database username and pw from secret manager |


## Requirement for Pipeline

1. Clone the repository
2. Install python3.7 or above in your environment
https://realpython.com/installing-python/
3. Install docker and docker-compose 
https://docs.docker.com/engine/install/
https://docs.docker.com/compose/install/
4. Install python package manager
https://pip.pypa.io/en/stable/installation/
5.Create python3 Virtual Envrioment
```
    python3 -m venv venv
    source venv/bin/activate
```
6. Install pyspark https://spark.apache.org/docs/latest/api/python/getting_started/install.html

```buildoutcfg
pip install pyspark
```

## Pipeline RunBook

Note : This example is not running spark cluster mode. It's running only standalone.So you no need to run using `spark-submit`


|Job Name | Execution Command  |
|---------|---------------------------------|
|`landing_to_processing_job.py` | |
|`processing_to_staging_job.py `| |
| `staging_to_dw_job.py`||



## Data Warehouse Model





