import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import concat_ws, upper
import boto3
import time
from pyspark.sql.functions import month, year, col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import split, col
from pyspark.sql.functions import format_string, col
from pyspark.sql.functions import count

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define S3 path of the CSV file
s3_path = "s3://bucketname/customers.csv"

# Read CSV file into a DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [s3_path]},
)

# Convert DynamicFrame to Spark DataFrame
df = dynamic_frame.toDF()

# Show schema and sample data
df.printSchema()
#df.show()

#read and print row count of the dataset
dflen = df.count()
print(f"Dataset length : {dflen}")

#read first row and print
firstrow = df.first()
print(f"first row values : {firstrow}")

# Create new column 'fullname' by concatenating 'firstname' and 'lastname'
df = df.withColumn("fullname", concat_ws(" ", df["First Name"], df["Last Name"]))

# Show updated DataFrame
# df.select("First Name", "Last Name", "fullname", "Subscription Date").show()

# convert fullname column to upper case
df = df.withColumn("fullname_upper", upper(df["fullname"]))

# Convert string to date format (adjust format if needed)
df = df.withColumn("Subscription Date", to_date(col("Subscription Date"), "dd-MM-yyyy"))

#df.select("First Name", "Last Name", "fullname", "fullname_upper", "Subscription Date").show()

# Extract month and year from 'Subscription Date'
df = df.withColumn("month", month(col("Subscription Date"))) \
       .withColumn("year", year(col("Subscription Date")))

#create new column with month and year combined
df = df.withColumn("month_year", concat_ws("-", format_string("%02d", month(col("Subscription Date"))), year(col("Subscription Date"))))

#extract http, https from website using split func
df = df.withColumn("Web_protocol", split(col("Website"), "://").getItem(0))

#df.select("First Name", "Last Name", "fullname", "fullname_upper", "Subscription Date", "month_year").show()

# Define your column rename mapping
rename_dict = {
    "month_year": "Month - Year",
    "Web_protocol": "Web Protocol",
    "fullname_upper": "Full name"
    # Add more pairs as needed: "old_name": "new_name"
}

# Apply renaming
for old_name, new_name in rename_dict.items():
    df = df.withColumnRenamed(old_name, new_name)

#group data based on month and year column
df_grouped = df.groupBy("month", "year", "Month - Year").agg(count("*").alias("total_records"))

df_grouped = df_grouped.orderBy("year", "month")

df_grouped = df_grouped.select("Month - Year", "total_records")

df_grouped.show()

#print selected columns
df.select("First Name", "Last Name", "Full name", "Subscription Date", "Month - Year", "Website", "Web Protocol").show()

df = df.select("First Name", "Last Name", "Full name", "Subscription Date", "Month - Year", "Website", "Web Protocol")

# Define output path in S3
output_path = "s3://bucketname/temp-csv-output/"
final_output_key = "final_output.csv"

print(f"output_path : {output_path}")
print(f"final_output_key : {final_output_key}")

# Write the DataFrame to CSV
# df.write.mode("overwrite") \
 #   .option("header", "true") \
 #  .csv(output_path)

df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

# Pause briefly to ensure files are written
time.sleep(5)

# Rename part file to final_output.csv using boto3
s3 = boto3.client("s3")
bucket = "bucketname"
prefix = "temp-csv-output/"

#get list of files from the specified bucket
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
part_file = None

#loop through the bucket list and check to see if it is file with csv extension
for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".csv"):
        part_file = key
        break

#if csv file founD in the above list, rename the file name as desired
if part_file:
    print(f"Renaming {part_file} to {final_output_key}")

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": part_file},
        Key=final_output_key
    )

    # Optional: clean up temp directory
    s3.delete_object(Bucket=bucket, Key=part_file)
    

