# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType
import json


tables = ['tbl_1','tbl_2']

def parse_json(json_str):
    if not json_str:
        return json_str  

    try:

        outer_json = json.loads(json_str)
        
        for key, value in outer_json.items():
            if isinstance(value, str):
                try:
                    
                    parsed = json.loads(value)
                    outer_json[key] = parsed  
                except json.JSONDecodeError:
                    continue  

        return json.dumps(outer_json)
    except json.JSONDecodeError:
        return json_str 

parse_json_udf = F.udf(parse_json, StringType())

def create_raw_tables(tbl_name):
    @dlt.table(name=f"_bronze_{tbl_name}")
    def st_autoloader():
        df = (spark.readStream.format("cloudFiles").option("cloudFiles.format","json")
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .option("cloudFiles.inferColumnTypes", "true")
                .load("/Volumes/ie_ui/airwx/raw_datasets/jsonexamplesimple3/")
                .withColumn("parsed_rescue", parse_json_udf(F.col("_rescued_data"))))
        return df

def process_schema_evolution(tbl_name):

    @dlt.table(name=f"_schema_evol_{tbl_name}")
    def mv_get_rescued_schema():
        return dlt.read(f"_bronze_{tbl_name}").selectExpr(f"schema_of_json_agg(parsed_rescue) as rescue_schema")
    
for tbl in tables:
    create_raw_tables(tbl)
    process_schema_evolution(tbl)

