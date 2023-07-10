The following project shows how to fetch some data (in this case formula one race results and qualifying results) from an online API and then do the ETL on the fetched data using AWS Glue. This project consists of two Jupyter Notebook files that are relatively easy to understand and are listed below:

```1. fetch_data.ipynb```

This file is used to fetch the data from the API. Since complete data is not available in one go, we make use of a for loop and a while loop to accumulate the data and once we have the data for one complete Formula One season we upload that data into our S3 Bucket.
Once the data has been uploaded to the S3 bucket, we can move on to the ETL processing that needs to be done on the data using AWS Glue.

```2. glue_etl_script.ipynb```

This script is used within AWS Glue to perform the ETL on the data that we fetched from the API. The data recovered from the API is in a deeply nested JSON file. First, we create two separate dynamic frames by loading the data from our S3 bucket and then converting those to spark dataframes as bulk of our unnesting process is done in spark. Once all the unnesting is done, we then correct the names of the columns of our data frame and finally write the data to the Glue and to our S3 bucket.
