#!/usr/bin/env bash

source $(dirname $0)/env.sh

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=SiteApplicationService" \
  -d '
  [
     {
        "tags":{
           "site":"sandbox",
           "application":"cassandraQueryLog"
        },
        "enabled": true,
        "config": "{}"
     }
  ]
  '

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=ApplicationDescService" \
  -d '
  [
     {
        "tags":{
           "application":"cassandraQueryLog"
        },
        "description":"cassandra Query Log Monitoring",
        "alias":"QueryLogMonitor",
        "groupName":"Cassandra",
        "config":"{}",
        "features":["common","metadata"]
     }
  ]
  '
## AlertStreamService
echo ""
echo "Importing AlertStreamService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" \
 -d '
 [
    {
       "tags":{
          "application":"cassandraQueryLog",
          "streamName":"cassandraQueryLogStream"
       },
       "description":"cassandra query log data source stream"
    }
 ]
 '
## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" \
 -d '
 [
    {
       "tags":{
          "application":"cassandraQueryLog",
          "alertExecutorId":"cassandraQueryLogExecutor",
          "streamName":"cassandraQueryLogStream"
       },
       "description":"executor for cassandra query log stream"
    }
 ]
 '
## AlertStreamSchemaService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
"http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" \
 -d '
 [
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "host"
       },
       "attrDescription": "the host that current metric comes form",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "source"
       },
       "attrDescription": "source host",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "user"
       },
       "attrDescription": "query user",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "timestamp"
       },
       "attrDescription": "query timestamp",
       "attrType": "long",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "category"
       },
       "attrDescription": "query category",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "type"
       },
       "attrDescription": "query type",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "ks"
       },
       "attrDescription": "query keyspace",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "cf"
       },
       "attrDescription": "query column family",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "operation"
       },
       "attrDescription": "query operation",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "masked_columns"
       },
       "attrDescription": "query masked_columns",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "cassandraQueryLog",
          "streamName": "cassandraQueryLogStream",
          "attrName": "other_columns"
       },
       "attrDescription": "query other_columns",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    }
 ]
 '

echo "Importing policy: capacityUsedPolicy "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
 -d '
 [
     {
		"tags": {
			"site": "sandbox",
			"application": "cassandraQueryLog",
			"policyId": "cf_ customer_details_and_ ks_ dg_keyspace_policy",
			"alertExecutorId": "cassandraQueryLogExecutor",
			"policyType": "siddhiCEPEngine"
		},
		"description": "Alert[cf_ customer_details_and_ ks_ dg_keyspace_policy]",
		"policyDef": "{\"expression\":\"from cassandraQueryLogStream[(cf == \\\"customer_details\\\") and (ks == \\\"dg_keyspace\\\")] select * insert into outputStream;\",\"type\":\"siddhiCEPEngine\"}",
		"dedupeDef": "{\"alertDedupIntervalMin\": 10,\"emailDedupIntervalMin\": 10}",
		"notificationDef": "[{\"sender\":\"no-reply@eagle.incubator.apache.org\",\"recipients\":\"no-reply@eagle.incubator.apache.org\",\"subject\":\"Alert[cf_ customer_details_and_ ks_ dg_keyspace_policy]\",\"flavor\":\"email\",\"id\":\"email_1\",\"tplFileName\":\"\"}]",
		"remediationDef": "",
		"enabled": true,
		"owner": "admin",
		"severity": 0,
		"markdownEnabled": false
	}
 ]'


# 1. Create required topic
# 2. Send some related events which may help to trigger alerts


## Finished
echo ""
echo "Done."