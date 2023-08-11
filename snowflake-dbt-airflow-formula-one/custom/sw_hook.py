from airflow.hooks.base import BaseHook
import snowflake.connector as snow

class SnowflakeHook(BaseHook):

    def __init__(self, account, user, password, warehouse, database, schema):
        self._account = account
        self._user = user
        self._pwd = password
        self._wh = warehouse
        self._db = database
        self._schema = schema

    def get_conn(self):

        conn = snow.connect(
            account = self._account,
            user = self._user,
            password = self._pwd,
            warehouse = self._wh,
            database = self._db,
            schema = self._schema
        )

        return conn
    
    def create_or_replace(self, conn, table_name, pandas_df):

        with conn.cursor() as c:
            try:
                c.execute(f"SELECT count(*) from FORMULA_ONE.FORMULA_ONE_SCHEMA.{table_name};")
                rows = c.fetchone()
            except conn.ProgrammingError:
                rows = 0
        if rows == 0:
            print("Creating Table .....")
            dtype_list = []
            for i in pandas_df.columns:
                if "date" in i.lower():
                    dtype_list.append([i,"DATE"])
                elif "time" in i.lower() or "q1" in i.lower() or "q2" in i.lower() or "q3" in i.lower():
                    dtype_list.append([i,"TIME"])
                elif "lat" in i.lower() or "long" in i.lower():
                    dtype_list.append([i,"VARCHAR"])
                elif  "_speed" in i.lower():
                    dtype_list.append([i,"VARCHAR"])
                else:
                    if pandas_df[i][0].replace(" ","").isalpha() or pandas_df[i][0].replace("_","").isalpha():
                        dtype_list.append([i,"VARCHAR"])
                    elif pandas_df[i][0].replace(".","").isnumeric():
                        dtype_list.append([i,"NUMERIC"])
            dtype_list = [" ".join(i) for i in dtype_list]
            columns = ", ".join(dtype_list)
            
            with conn.cursor() as c:
                c.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});")

            print("Table Created.")
        else:
            print("Table already exists.")

