# Databricks notebook source

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
            "new_attribute2_true_json":{"k1":{"x1":2}}
        }
# spark.createDataFrame([jd]).write.mode("overwrite").format("json").save("/Volumes/ie_ui/airwx/raw_datasets/jsonexamplesimple3/")
spark.createDataFrame([jd]).write.mode("append").format("json").save("/Volumes/ie_ui/airwx/raw_datasets/jsonexamplesimple3/")
