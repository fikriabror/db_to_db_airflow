# ETL Data Processing with PostgreSQL

## Project Overview

This project implements an Extract, Transform, Load (ETL) pipeline using Apache Airflow and PostgreSQL. 
specifically focusing on loading data into a PostgreSQL database and performing necessary transformations.

## Prerequisites

- You can see the prerequisites in the requirements.txt

## Sample Data

The project uses sample data sourced from Kaggle. You can download the dataset from [Kaggle](https://www.kaggle.com/datasets/utkarshx27/failed-banks-database)
I have provided the data inside the `data` folder within the project directory with some modifications to support the transformation.

## How to Run
- First you have to import data in your postgres database local machine,
- Run the docker-compose up --build
- Visit your http://localhost:8080/
- Registered the airflow connection called source_postgre
- Next step need to add another database and registered the db in the airflow connection called dest_postgre
- Run the DAG tasks
