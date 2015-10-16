#!/bin/sh
#### HiveResourceSensitivityService
curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=HiveResourceSensitivityService" -d '[{"prefix":"hiveResourceSensitivity","tags":{"hiveResource":"xademo.customer_details.phone_number"},"sensitivityType":"PHONE_NUMBER"}]'