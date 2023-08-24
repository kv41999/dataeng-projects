# INTRODUCTION
The project **snowflake-dbt-airflow-formula-one** aims to present a structured method of Data Ingestion, Transformation and Loading using the popular tools such as **Apache Airflow**, **Apache Spark** and **Snowflake**.

The ingestion part is handled using the ```requests``` library native to python and storing data locally for this project which can be changed to using cloud storage options like S3, etc.

The transformation part is handled using **Apache Spark** and the warehousing or loading of data is done in **Snowflake**.

All these processes are glued together by a data pipeline orchestration tool called **Apache Airflow**.

More info about various files present in this project are below.


## dag-snowflake-formula-one4.py

This python file is the main file where the whole DAG comes together and dependencies between various tasks is defined.

We use 2 custom operators for fetching and transforming the data, a custom sensor used to check when both previous or upstream tasks are completed in order to progress further and a custom hook 
to connect and upload data to Snoflake.

The data we are using here is the Formula One Data from [Ergast Developer API](http://ergast.com/mrd/) which is a free developer API for fetching data.

I require data for every year starting from 2010 upto 2020, hence the starting date and the schedule is set to yearly as I am bulk downloading the data for every year.

There are two separate tasks for fetching the actual race data and qualifying data, then there is the sensor that makes sure that both the files required are in the local storage and finally the transform and load operations are together in one task which can be separated but would require more storage options.


## sw_hook.py

This is a custom hook used to connect to Snowflake data warehouse.

We are getting all the credentials from **Airflow Connections** where all the sensitive data has been stored safely.

Then we have a ```get_conn()``` method to create a session and connect to Snowflake.

Another method we have is ```create_or_replace()``` which uses the session that we have created to create a table in our database and schema if it doesn't exist based on the columns that our data from the Ergast Developer API has.


## data_fetch_op.py

This is a custom operator for fetching the data and storing it into our local system or alternatively in a cloud storage solution.

We, first of all, are using the **start_date** or the **execution_date** to fetch the date from our API. Since, we require data for a whole year and you are unable to get all of that in one go, I am using a while loop to compile the complete data for a particular year using different parameters such as offset and limit.

**url_query** is basically an additional parameter that we have to add in order to define whether we want race results or qualifying results.

Once the data is compiled, it is stored in a **JSON** format into the local storage.

Finally, once the data is stored, we create an **XCOM** with a key-value pair of the year for which we fetched the data which will prove useful in the further two tasks in order to identify the files.


## data_fetch_sensor.py

This is a simple custom sensor which checks for whether both race and qualifying data is available before moving forward. The ```poke()``` method in the sensor keeps checking for both files and until both of them are present in the storage, it won't move forward to the downstream task for transforming data since it depends on both the files.

Although we can separate this process, so that there is no dependency between race results and qualifying results but logically it makes sense to run transformations on both the data files together since we don't want one type of data to lag or lead the other type of data. It makes sense to have both race and qualifying results for a particular year at the same time rather than running one task afterwards if one of the files fails to be fetched.


## data_TL_op.py

This is another custom operator used for transforming the JSON data into a spark dataframe which will further be transformed into a pandas dataframe in order to be loaded into snowflake using ```write_pandas()``` method.

First, we pull the year value from **XCOM** in order to be used to read the JSON files into a Spark DataFrame.

We then read the dataframes and pass them through a custom function I built called ```array_unnesting()``` which internally make a call to another method called ```struct_unravel()```. Both these functions are used to unnest the complex multiline JSON data and create columns of every keys and this is where the power of Spark comes into play. Along with this we rename our columns and remove some unnecessary columns that include URLs using the ```column_name_correction()``` method.

Once the data is untangled and the columns have been renamed, we then proceed to fetch the Connection credentials and use them with our custom hook called ```SnowflakeHook``` in order to start a snowflake session.

We then convert our Spark Dataframe into a Pandas Dataframe and clean the Race and Qualifying data separately as both these tables have different columns that require separate attention.

After cleaning the data, we use the ```create_or_replace()``` method defined in our hook to create a table based on the columns we have, if the table already exists we directly proceed to loading the data.

Finally, we load the data into our Snowflake Data Warehouse to be further modelled by **DBT**.