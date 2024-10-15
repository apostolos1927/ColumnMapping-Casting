# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

# Sample data
schema = ["id", "age", "name", "int_str1", "int_str2", "timestamp_str"]
data = [
    (1, 25, "John", "100", "200", "2021-01-01 12:00:00"),
    (2, 30, "Jane", "150", "250", "2021-01-02 13:00:00"),
    (3, 35, "Doe", "175", "275", "2021-01-03 14:00:00")
]
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame and its schema
df.printSchema()
df.display()
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable('demo')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo RENAME COLUMN age TO age_in_years

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo DROP COLUMN name

# COMMAND ----------

# MAGIC %sql
# MAGIC  ALTER TABLE demo SET TBLPROPERTIES (
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.minReaderVersion' = '2',
# MAGIC             'delta.minWriterVersion' = '5')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, TimestampType, StringType


def cast_columns(df, cast_map):
    for col_name, new_type in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(new_type))
    return df


cast_map = {
    "int_str1": IntegerType(),
    "int_str2": IntegerType(),
    "timestamp_str": TimestampType()
}

df = spark.table("demo")
df_casted = cast_columns(df, cast_map)

df_casted.printSchema()
df_casted.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as F 

def combined_methods(table: str, datatypes: dict = {},columns_to_rename: dict = {}, columns_to_drop: list=[]) -> None:
    #cast columns
    cols = {
        col: F.col(col).cast(datatype) if col not in columns_to_drop else F.lit(None).cast(datatype)
        for col, datatype in datatypes.items()
    }
    print('cols is ',cols)
    df = spark.read.table(table)
    df = df.withColumns(cols)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    #enable column mapping
    spark.sql(f"""
        ALTER TABLE {table} SET TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5')
        """
    )
    #rename columns
    for old_col, new_col in columns_to_rename.items():
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")
    #drop columns
    print(f"ALTER TABLE {table} DROP COLUMNS ({', '.join([col for col in columns_to_drop])})")
    spark.sql(f"ALTER TABLE {table} DROP COLUMNS ({', '.join([col for col in columns_to_drop])})")
    
    
if __name__ == "__main__":
    table = 'demo'
    new_datatypes = {
        "age": "integer",
        "int_str1": "integer",
        "int_str2": "integer",
        "timestamp_str" : "timestamp",
      }
    cols_to_drop = ["name"]
    columns_renamings = {"id": "user_id", "age": "user_age"}
    
    combined_methods(table,new_datatypes,columns_renamings,cols_to_drop)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo
