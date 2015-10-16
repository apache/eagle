# HDFS Resource Access Config 
curl -X POST -H 'Content-Type:application/json' "http://localhost:8080/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix":"alertDataSource","tags":{"site":"cluster1-dc1","dataSource":"hdfsAuditLog"},"config":"{\"hdfsEndpoint\":\"hdfs://sandbox.hortonworks.com:8020\"}" }]'

