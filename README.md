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
Index,Customer Id,First Name,Last Name,Company,City,Country,Phone 1,Phone 2,Email,Subscription Date,Website
1,EB54EF1154C3A78,Heather,Callahan,Mosley-David,Lake Jeffborough,Norway,043-797-5229,915.112.1727,urangel@espinoza-francis.net,26-08-2020,http://www.escobar.org/
2,10dAcafEBbA5FcA,Kristina,Ferrell,"Horn, Shepard and Watson",Aaronville,Andorra,932-062-1802,(209)172-7124x3651,xreese@hall-donovan.com,27-04-2020,https://tyler-pugh.info/
3,67DAB15Ebe4BE4a,Briana,Andersen,Irwin-Oneal,East Jordan,Nepal,8352752061,(567)135-1918,haleybraun@blevins-sexton.com,22-03-2022,https://www.mack-bell.net/
4,6d350C5E5eDB4EE,Patty,Ponce,Richardson Group,East Kristintown,Northern Mariana Islands,302.398.3833,196-189-7767x770,hohailey@anthony.com,02-07-2020,https://delacruz-freeman.org/
5,5820deAdCF23EFe,Kathleen,Mccormick,Carson-Burch,Andresmouth,Macao,001-184-153-9683x1497,552.051.2979x342,alvaradojesse@rangel-shields.com,17-01-2021,https://welch.info/
6,E1CDEaC63fDd5aA,Trevor,Lee,Maddox Group,Lake Madelineburgh,Senegal,+1-134-348-0265x9132,-5361,briangriffin@chang.org,13-08-2021,https://www.roberts.com/
7,3e1187fCcebC8d2,Mathew,Hoffman,"Bender, Pittman and Kidd",West Ralph,Uzbekistan,842.380.1168,(178)178-5447x32603,bauergerald@morrison.com,09-04-2020,http://www.holt.com/
8,47C5cEE243c9A7b,Glenn,Wiggins,Glenn-Harvey,Ambershire,Falkland Islands (Malvinas),245-207-5608x563,8806867785,changkellie@howell.com,02-04-2021,http://carlson.com/
9,cacaD68a5e4BF4b,Bruce,Payne,"Arroyo, Cain and Hudson",Barrettview,Zimbabwe,391.313.4649x42910,166.227.5055,mayerjerome@hurst-graham.net,26-11-2020,https://www.glenn-snow.com/
10,436b9c41cfb1fa3,Brendan,Franco,"Schaefer, Blair and Shaffer",New Rickey,Ukraine,001-315-224-3556,254-621-7128x7573,kentryan@stone-oneill.com,29-06-2021,http://ruiz.com/

## Output Example
Sample output is a grouped stats based on subscription month and year. A sample csv file is created with selected columns along with transformations.

## How to Run
This script is designed to run inside AWS Glue, but can be adapted to run locally with a Spark environment.

## Author
Shailesh Mistry – [LinkedIn](https://www.linkedin.com/in/shailesh-mistry-a346659)
