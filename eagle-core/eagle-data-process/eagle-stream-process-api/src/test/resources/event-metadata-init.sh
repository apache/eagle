#!/usr/bin/env bash

export EAGLE_SERVICE_USER="admin"
export EAGLE_SERVICE_PASSWD="secret"
export EAGLE_SERVICE_HOST="localhost"
export EAGLE_SERVICE_PORT=38080

# AlertDataSource: data sources bound to sites
echo "Importing AlertDataSourceService for stream... "

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix":"alertDataSource","tags":{"site" : "sandbox", "dataSource":"eventSource"}, "enabled": "true", "config" : ""}]'

## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for stream... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"dataSource":"eventSource","streamName":"eventStream"},"desc":"alert event stream from hdfs audit log"}]'

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for stream ... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"eventSource","alertExecutorId":"eventStreamExecutor","streamName":"eventStream"},"desc":"alert executor for event stream"}]'

## AlertStreamSchemaService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for stream ... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"dataSource":"eventSource","streamName":"eventStream","attrName":"timestamp"},"attrDescription":"event timestamp","attrType":"long","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"eventSource","streamName":"eventStream","attrName":"name"},"attrDescription":"event name","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"eventSource","streamName":"eventStream","attrName":"value"},"attrDescription":"event value","attrType":"integer","category":"","attrValueResolver":""}]'