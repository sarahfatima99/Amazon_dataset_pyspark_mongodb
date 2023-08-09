# Amazon_dataset_pyspark_mongodb

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Database setup and environment varialbles](#Seting up database and environment variables)


## Description:

This project includes data pipeline built using python that takes amazon dataset through pyspark, apply transformations and then loads into mongodb. Before loading, the data is passed through some validation checks.Email alert is generated when any anomali is found

## Installation:

Follow the instruction for project setup:
1. clone the repository : https://github.com/sarahfatima99/Amazon_dataset_pyspark_mongodb
2. cd Amazon_dataset_pyspark_mongodb
3. pip install -r requirements.txt
4. download mongodb using the url https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-6.0.8-signed.msi

## Seting up database and environment variables:

1. Add .env file in your project
2. Add following variables and their respective values (example)
    DATABASE = database
    PASSWORD = password
    EMAIL_ADDRESS = emmail@gmail.com



