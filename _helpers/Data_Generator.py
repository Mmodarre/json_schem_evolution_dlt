# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

folder = "/Volumes/databricks_demo_catalog/databricks_demo_schema/demo_volume"
from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict
import uuid
fake = Faker()
import random
fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date_last_year = F.udf(lambda:fake.date_time_between('-2y','-2M').strftime("%m-%d-%Y %H:%M:%S"))
fake_date_this_month = F.udf(lambda:fake.date_time_between('-1M','-1d').strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
new_customer = F.udf(fake.pyint)
operations = OrderedDict([("DELETE", 0.1),("UPDATE", 0.89),(None, 0.01)])
fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)



# COMMAND ----------

CustNumToGenerate = 2
max_cust_id = None
try:
  max_cust_id = spark.read.table("databricks_demo_catalog.databricks_demo_schema.customers_bronze").selectExpr("max(customers_id)").collect()[0][0]

  if max_cust_id is not None:
      max_cust_id = int(max_cust_id[1:]) + 1
  else:
    max_cust_id = 0
except:
  max_cust_id = 0


df = spark.range(max_cust_id+1, max_cust_id+1+CustNumToGenerate)
df = df.withColumn("customers_ID",F.concat(F.lit('S'), F.col("id").cast("String")))
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
#df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("operation", F.lit('APPEND'))
df = df.withColumn("new_customer",F.lit(fake.pybool()))
df = df.withColumn("hash",F.lit(F.md5(fake_email())))
df = df.withColumn("pk_hash",F.lit(F.md5(F.concat(F.lit('C'), F.col("id").cast("String")))))
df= df.withColumn("insert_date",fake_date_this_month())
df = df.withColumn("batch_id",F.lit(fake.pyint(2232,1123423423,1)))
df_customers = df.withColumn("operation_date", fake_date_last_year())
df_customers = df_customers.drop('id')


df_customers.write.format("json").mode("overwrite").save(folder+"/customers_json")


# COMMAND ----------


