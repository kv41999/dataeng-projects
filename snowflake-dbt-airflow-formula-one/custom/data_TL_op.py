from airflow.models import BaseOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import snowflake.connector.pandas_tools as spd
from custom.sw_hook import SnowflakeHook
from airflow.utils.decorators import apply_defaults
from airflow.models import Connection
import re
from datetime import datetime as dt
from datetime import timedelta as td

sc = SparkSession.builder.appName("data_transform").getOrCreate()

class FormulaOneDataTransformer(BaseOperator):

    @apply_defaults
    def __init__(self, race_input_path, quali_input_path, **kwargs):
        super(FormulaOneDataTransformer,self).__init__(**kwargs)
        self.race_input_path = race_input_path
        self.quali_input_path = quali_input_path

    
    def execute(self, context):

        ti = context["ti"]
        start_year = ti.xcom_pull("Race_data_fetch","FormulaOneETL4","year")

        race_df = sc.read.format("json").option("path",self.race_input_path + f"/race_{start_year}.json").load()
        quali_df = sc.read.format("json").option("path",self.quali_input_path + f"/quali_{start_year}.json").load()
        race_df = self.array_unnesting(race_df)
        quali_df = self.array_unnesting(quali_df)
        race_df = self.column_name_correction(race_df)
        quali_df = self.column_name_correction(quali_df)
        
        conn_air = Connection.get_connection_from_secrets("sw_default")
        account = conn_air.extra_dejson.get("account")
        username = conn_air.login
        pwd = conn_air.password
        warehouse = conn_air.extra_dejson.get("warehouse")

        sw = SnowflakeHook(account, username, pwd, warehouse,"FORMULA_ONE","FORMULA_ONE_SCHEMA")
        conn = sw.get_conn()

        race_df_pd = race_df.toPandas()
        self.race_data_cleaning(race_df_pd)
        
        quali_df_pd = quali_df.toPandas()
        self.quali_data_cleaning(quali_df_pd)

        sw.create_or_replace(conn, "FACT_RACE", race_df_pd)
        sw.create_or_replace(conn, "FACT_QUALI", quali_df_pd)

        spd.write_pandas(conn, race_df_pd, "FACT_RACE", "FORMULA_ONE", "FORMULA_ONE_SCHEMA",overwrite=False)
        spd.write_pandas(conn, quali_df_pd, "FACT_QUALI", "FORMULA_ONE", "FORMULA_ONE_SCHEMA",overwrite = False)

    
    def race_data_cleaning(self,df):
        df.columns = map(str.upper, df.columns)
        df.drop("RESULTS_POSITIONTEXT",axis=1,inplace=True)
        df["RESULTS_STATUS"] = df["RESULTS_STATUS"].map(lambda x: "Not Finished" if x != "Finished" else x)
        df["TIME"] = df["TIME"].map(lambda x: x.replace("Z",""))
        df["FASTESTLAP_TIME_TIME"] = df["FASTESTLAP_TIME_TIME"].map(lambda x: dt.strftime(dt.strptime(x,"%M:%S.%f"),"%H:%M:%S.%f") if x else x) 
        top_time = 0
        for i in range(df.shape[0]):
            if df["RESULTS_TIME_TIME"][i]:
                if re.match("\d+:\d+:\d+.\d+",df["RESULTS_TIME_TIME"][i]):
                    top_time = dt.strptime(df["RESULTS_TIME_TIME"][i],"%H:%M:%S.%f")
                    df["RESULTS_TIME_TIME"][i] = top_time.strftime("%H:%M:%S.%f")
                else:
                    find = re.search("(\d+):(\d+).(\d+)",df["RESULTS_TIME_TIME"][i])
                    if not find:
                        find = re.search("(\d+).(\d+)",df["RESULTS_TIME_TIME"][i])
                    try:
                        minu = find.group(1)
                        s = find.group(2)
                        milli = find.group(3)
                    except IndexError:
                        minu = 0
                        s = find.group(1)
                        milli = find.group(2)
                    
                    delta = td(minutes=int(minu), seconds = int(s), milliseconds=int(milli))
                    new_time = top_time + delta
                    df["RESULTS_TIME_TIME"][i] = new_time.strftime("%H:%M:%S.%f")

    
    def quali_data_cleaning(self,df):
        df.columns = map(str.upper, df.columns)
        df["TIME"] = df["TIME"].map(lambda x: x.replace("Z",""))
        df["QUALIFYINGRESULTS_Q1"] = df["QUALIFYINGRESULTS_Q1"].map(lambda x: dt.strftime(dt.strptime(x,"%M:%S.%f"),"%H:%M:%S.%f") if x and x != "" else None)
        df["QUALIFYINGRESULTS_Q2"] = df["QUALIFYINGRESULTS_Q2"].map(lambda x: dt.strftime(dt.strptime(x,"%M:%S.%f"),"%H:%M:%S.%f") if x and x != "" else None) 
        df["QUALIFYINGRESULTS_Q3"] = df["QUALIFYINGRESULTS_Q3"].map(lambda x: dt.strftime(dt.strptime(x,"%M:%S.%f"),"%H:%M:%S.%f") if x and x != "" else None)

    def column_name_correction(self,df):    
        for i in df.columns:
            if "_url" in i:
                df = df.drop(i)
            else:
                temp = i.split("_")
                if "time" in i.lower():
                    df = df.withColumnRenamed(i,"_".join(temp[len(temp)-3:]).lower())
                else:
                    df = df.withColumnRenamed(i,"_".join(temp[len(temp)-2:]).lower())
        return df    


    def struct_unravel(self, nested_df):

        list_schema = [((), nested_df)]
        flat_columns = []
        
        while len(list_schema) > 0:
            parents, df = list_schema.pop()
            flat_cols = [  col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) for c in df.dtypes if c[1][:6] != "struct"   ]
        
            struct_cols = [  c[0]   for c in df.dtypes if c[1][:6] == "struct"   ]
        
            flat_columns.extend(flat_cols)
            for i in struct_cols:
                    projected_df = df.select(i + ".*")
                    list_schema.append((parents + (i,), projected_df))
        
        return nested_df.select(flat_columns)
    

    def array_unnesting(self, df):
        array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
        while len(array_cols)>0:
            for c in array_cols:
                df = df.withColumn(c,explode_outer(c))
            df = self.struct_unravel(df)
            array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
        return df