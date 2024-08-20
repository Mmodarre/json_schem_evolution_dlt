# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC If the actual data exists in the form of JSON payload, then it makes more sense to store it as JSON file instead of storing it as a string blob in Parquet format. 
# MAGIC
# MAGIC In the first case, Spark can infer schema more smartly at all levels of nesting with relevant types like primitive types (string, int etc) or complex e.g. structs). Also, schema evolution is streamlined as well if the data is stored as JSON format, as any new field could be identified and processed.
# MAGIC
# MAGIC If JSON data is stored as a blob within a string column in formats like Parquet, Spark doesn't attempt to infer its schema. Instead, it treats it as string, like any other primitive string. And if any new fields appear in such string-quoted-json, Spark can't discover. Such string-quoted-json aka json blobs can still be converted to Struct but it introduces another pre-processing step. 
# MAGIC
# MAGIC The pre-processing step to parse JSON blob in Parquet to proper struct introduces challenges when operating in Streaming mode (e.g. in a DLT pipeline). The primary way to parse a JSON blob to JSON struct is via from_json method which requires schema to be statically passed. This leads to two potential options:
# MAGIC 1. Either define the schema before hand statically and pass it to the function. For an evolving schema, this option is not feasible.
# MAGIC 2. Look at an example row of the data and use schema_of_json on it and then pass the resultant value. In streaming pipelines, this is not possible as it requires invoking actions like .first() or .collect() which are not supported in streaming.
# MAGIC 3. Infer schema dynamically via the from_json PrPr but this requires pipelines to be restarted every time a schema changes which can be an overhead.
# MAGIC
# MAGIC Thus, it's possible to acquire raw data as JSON and configure AutoLoader to process JSON files, infer schema dynamically. 
# MAGIC
# MAGIC This notebook includes a few examples to substantiate this point.

# COMMAND ----------

# consider that our raw data looks like the following JSON.

null = true = false = "something"
jd = {
            "payment_intent_id": "int_2",
            "merchant_request_id": "60e27376-a280-44e3-8096-68f151214068",
            "total_amount": 1000,
            "currency": "HKD",
            "merchant_account": "06a29c7f-4792-49b2-b916-ea7f538b0111",
            "inbound_request_id": "4179e177-f9df-4da0-8f79-ce4d1e821689",
            "order_data": '{"products":[{"code":"3414314111","desc":"test desc","name":"IPHONE7","quantity":5,"sku":"piece","type":"physical","unitPrice":10,"url":"test_url"}],"type":"v_goods"}',
            "payment_methods": null,
            "additional_data": "{}",
            "created": 1720725297563818,
            "updated": 1720725298576956,
            "live_mode": true,
            "attempt_id": "att_sgsth5h94gxy2aex50j_adtdon",
            "customer_id": null,
            "email": "",
            "phone": null,
            "next_action": null,
            "merchant_order_id": "81dea78f-2220-4223-b0b2-22cf4d3f0cd9",
            "captured_amount": 0,
            "attempt_method": "visa",
            "status": "REQUIRES_PAYMENT_METHOD",
            "version": 1,
            "descriptor": "vip8888",
            "metadata": null,
            "cancelled": null,
            "cancellation_reason": null,
            "supplementary_amount": null,
            "foreign_exchange_transaction_type": null,
            "return_url": null,
            "payment_contract_id": null,
            "payment_consent_id": null,
            "attributes": '{"referrerData":{"type":"N/A"}}',
            "commission": null,
            "platform_account": null,
            "skip_risk_processing": true,
            "force_3ds": false,
            "connected_account": null,
            "external_3ds": false,
            "skip_3ds": false,
            "card_details_received_via": null,
            "card_input_via": null,
            "customer": null,
            "payment_method_options": null,
            "tra_applicable": false,
            "reference": null,
            "message": null,
            "base_amount": 1000,
            "surcharge_fees": null,
            "webhook_url": null,
            "internal_metadata": '{"airway":"{\\"user_id\\":\\"37bc1628-4860-4e01-b4cf-0c803c82cb96\\",\\"token_type\\":\\"client\\",\\"remote_addr\\":\\"35.240.211.132\\",\\"operator_account_id\\":\\"06a29c7f-4792-49b2-b916-ea7f538b0111\\",\\"operator_account_type\\":\\"account\\"}","awx-exec-account-id":"06a29c7f-4792-49b2-b916-ea7f538b0111","merchant_id":"06a29c7f-4792-49b2-b916-ea7f538b0111","resource_id":"int_sgsth5h94gxy2adtdon","resource_trace_id":"adtdon","userId":"37bc1628-4860-4e01-b4cf-0c803c82cb96"}',
            "base_currency": "HKD",
            "funds_split_data": null,
            "migrated": null,
            "new_attribute1_string_json":'{"referrerData":{"type":"N/A"}}',
            "new_attribute2_true_json":{"k1":{"x1":"v1"}}
        }
# # spark.createDataFrame([jd]).write.mode("overwrite").format("json").save("/Volumes/ie_ui/airwx/raw_datasets/jsonexamplesimple3/")
# spark.createDataFrame([jd]).write.mode("append").format("json").save("/Volumes/ie_ui/airwx/raw_datasets/jsonexamplesimple3/")

# COMMAND ----------

# lets firstly store it as a string-quoted-json or json blob in a Parquet format and read it back:
import json
jd_str = json.dumps(jd)
df = spark.createDataFrame([(jd_str)],['value'])
df.display()

# COMMAND ----------

df.write.format("parquet").mode("overwrite").save("/Volumes/ie_ui/airwx/raw_datasets/demo_dataset_parquet_sample/")

# COMMAND ----------

df_pq = spark.read.parquet("/Volumes/ie_ui/airwx/raw_datasets/demo_dataset_parquet_sample/")
df_pq.display()

# COMMAND ----------

# inferring schema would require the following steps:
import pyspark.sql.functions as F
value_schema = df.selectExpr("schema_of_json_agg(value) as value_schema").first()['value_schema']
df.selectExpr(f"from_json(value,'{value_schema}') as value_struct").select("value_struct.*").display()

# note that we have used .first() here which can't be used in streaming. 

# COMMAND ----------

# on the other hand, if we had stored the original json data as JSON:
spark.createDataFrame([jd]).write.mode("overwrite").format("json").save("/Volumes/ie_ui/airwx/raw_datasets/demo_dataset_json_sample/")

# COMMAND ----------

# and read the json data again: 
df_json = spark.read.json("/Volumes/ie_ui/airwx/raw_datasets/demo_dataset_json_sample/")
df_json.display()

# COMMAND ----------

# we can achieve the desired table structure without requiring any streaming incompatible pre-processing as was required for Parquet data.

# COMMAND ----------

{
    "id": "95a97351-efda-4b4d-a0ec-ac9711270e3b",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "instance_pool_id": "0719-040640-noble119-pool-q2q6878f",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 2,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "CURRENT",
    "photon": true,
    "libraries": [
        {
            "notebook": {
                "path": "notebook_path"
            }
        }
    ],
    "name": "ie_airwx_demo_preprocessing",
    "edition": "ADVANCED",
    "catalog": "ie_ui",
    "target": "airwx_demo",
    "data_sampling": false
}
