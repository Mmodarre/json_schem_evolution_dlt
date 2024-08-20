# Databricks notebook source
# MAGIC %md
# MAGIC # Customer dummy data generator
# MAGIC This Notebook generates dummy customer data to JSON format to ingest using Delta Live Tables to Bronze and Silver Delta tables. It also generates Update/Delete rows for existing data to demo the capability of performing Slowly Chaning Dimensions(SCD) Type 1/2.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Faker library

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup variables
# MAGIC - Setup raw data location.
# MAGIC - Unity catalog Schema and Catalog
# MAGIC - Bronze and Silver tables.

# COMMAND ----------

## Location of the raw data written by generator
folder = "/Volumes/databricks_demo_catalog/databricks_demo_schema/demo_volume"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create some spark UDF functions to generate data fields

# COMMAND ----------

from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict
import uuid
import random

fake = Faker()
fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_ip = F.udf(fake.ipv4_public)
fake_login_country = F.udf(fake.country)
fake_date_last_year = F.udf(lambda:fake.date_time_between('-2y','-2M').strftime("%m-%d-%Y %H:%M:%S"))
fake_date_this_month = F.udf(lambda:fake.date_time_between('-1M','-1d').strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_address_country = F.udf(fake.country)
fake_address_city = F.udf(fake.city)
fake_addresse_zip = F.udf(fake.zipcode)
fake_address_street = F.udf(fake.street_address)
## Operations => 10 percent delete, 90 percent update
operations = OrderedDict([("DELETE", 0.1),("UPDATE", 0.9),(None, 0.00)])
fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)



# COMMAND ----------

# MAGIC %md
# MAGIC ##Main Python code to generate customer data

# COMMAND ----------

## Adjust to generate more or less data
CustNumToGenerate = 100

batch_id = F.lit(fake.pyint(2232, 1123423423, 1))
max_cust_id = None
try:
    max_cust_id = (
        spark.read.table("databricks_demo_catalog.databricks_demo_schema.silver_tbl_1")
        .selectExpr("max(CAST(SUBSTRING(customer_id, 2) AS INT))")
        .collect()[0][0]
    )

    if max_cust_id is not None:
        max_cust_id = max_cust_id
    else:
        max_cust_id = 0
except:
    max_cust_id = 0

df = spark.range(max_cust_id + 1, max_cust_id + 1 + CustNumToGenerate)
df = df.withColumn("customer_id", F.concat(F.lit("S"), F.col("id").cast("String")))
df = df.withColumn(
    "personal_info",
    F.struct(
        fake_firstname().alias("firstname"),
        fake_lastname().alias("lastname"),
        F.struct(
            fake_address_street().alias("street_address"),
            fake_address_city().alias("city"),
            fake_addresse_zip().alias("zip"),
            fake_address_country().alias("country"),
        ).alias("address"),
    ),
)
df = df.withColumn("operation", F.lit("APPEND"))
df = df.withColumn("operation_date", fake_date_last_year())
df = df.withColumn(
    "activity",
    F.struct(
        F.struct(
            fake_date_this_month().alias("latest_login"),
            fake_ip().alias("last_loggin_IP"),
        ).alias("login")
    ),
)
df = df.withColumn("batch_id", batch_id)
df = df.drop("id")
display(df)

update_delete_df = (
    spark.read.table("databricks_demo_catalog.databricks_demo_schema.silver_tbl_1")
    .selectExpr("customer_id")
    #.filter("__End_at is null") // Uncomment if you are doing SCD Type 2 at Silver layer
    .sample(False, 0.05)
    .withColumn(
        "personal_info",
        F.struct(
            fake_firstname().alias("firstname"),
            fake_lastname().alias("lastname"),
            F.struct(
                fake_address_street().alias("street_address"),
                fake_address_city().alias("city"),
                fake_addresse_zip().alias("zip"),
                fake_address_country().alias("country"),
            ).alias("address"),
        ),
    )
    .withColumn("operation", fake_operation())
    .withColumn("operation_date", fake_date_this_month())
    .withColumn(
    "activity",
    F.struct(
        F.struct(
            fake_date_this_month().alias("latest_login"),
            fake_ip().alias("last_loggin_IP"),
        ).alias("login")
    ),
)
    .withColumn("batch_id", batch_id)
)
display(update_delete_df)

df = df.union(update_delete_df)
display(df)
#df.write.mode('append').json(folder+"/customers_json")
