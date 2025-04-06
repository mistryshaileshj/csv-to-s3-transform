# AWS Glue ETL Script: Customer Data Transformation

This project demonstrates an AWS Glue ETL script that:
- Reads customer data from an S3 bucket (CSV format)
- Transforms the data by:
  - Concatenating first and last names
  - Converting names to uppercase
  - Extracting month and year from subscription dates
  - Split column value
  - Renaming columns
  - Formatting date
  - Read and display first row in the dataset
  - Grouping by month/year
- Writes the transformed output back to S3
- Renames the output CSV using Boto3

## Technologies
- AWS Glue (PySpark)
- AWS S3
- Python (Boto3)

## Sample Input
Provide a sample of your CSV file or a few rows here.

## Output Example
Sample output is a grouped stats based on subscription month and year. A sample csv file is created with selected columns along with transformations.

## How to Run
This script is designed to run inside AWS Glue, but can be adapted to run locally with a Spark environment.

## Author
Shailesh Mistry – [LinkedIn](https://www.linkedin.com/in/shailesh-mistry-a346659)
