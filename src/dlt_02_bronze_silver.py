# Databricks notebook source
import dlt
import pyspark.sql.functions as F

tables = ['tbl_1','tbl_2']

preprocessing_catalog = spark.conf.get("preprocessing_catalog")
preprocessing_schema = spark.conf.get("preprocessing_schema")


def create_bronze_table(tbl_name):
    @dlt.table(name=f"bronze_{tbl_name}")
    def table():
      df_st = spark.readStream.table(f"{preprocessing_catalog}.{preprocessing_schema}._bronze_{tbl_name}")
      rescue_schema = spark.table(f"{preprocessing_catalog}.{preprocessing_schema}._schema_evol_{tbl_name}").first()['rescue_schema']
      if rescue_schema == 'STRING':
        return df_st

      else:
        return df_st.withColumn("struct_rescue",F.from_json("parsed_rescue",rescue_schema)).select("*","struct_rescue.*")

def merge_bronze_silver(tbl_name):
  dlt.create_streaming_table(f"silver_{tbl_name}")

  dlt.apply_changes(
    target = f"silver_{tbl_name}",
    source = f"bronze_{tbl_name}",
    keys = ["customer_id"],
    sequence_by = F.col("operation_date"),
    stored_as_scd_type = 1
  )

    
for tbl in tables:
    create_bronze_table(tbl)
    merge_bronze_silver(tbl)
