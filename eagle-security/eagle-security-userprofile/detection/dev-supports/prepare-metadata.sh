#!/usr/bin/env bash

export SERVICE_HOST="localhost"
export SERVICE_PORT=9099

echo "Creating alert definition for AlertDefinitionService with userprofile-policy-definition.json"
result=$(curl -XPOST -H "Content-Type: application/json" \
        "http://$SERVICE_HOST:$SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
        -d '[ { "prefix": "alertdef", "tags": { "site": "sandbox", "dataSource": "userProfile", "alertExecutorId": "userProfileAnomalyDetectionExecutor", "policyId": "userProfile", "policyType": "MachineLearning" }, "desc": "user profile anomaly detection", "policyDef": "{\"type\":\"MachineLearning\",\"alertContext\":{\"site\":\"sandbox\",\"dataSource\":\"userProfile\",\"component\":\"testComponent\",\"description\":\"ML based user profile anomaly detection\",\"severity\":\"WARNING\",\"notificationByEmail\":\"true\"},\"algorithms\":[{\"name\":\"EigenDecomposition\",\"evaluator\":\"eagle.security.userprofile.impl.UserProfileAnomalyEigenEvaluator\",\"description\":\"EigenBasedAnomalyDetection\",\"features\":\"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete, setOwner, fsck\"},{\"name\":\"KDE\",\"evaluator\":\"eagle.security.userprofile.impl.UserProfileAnomalyKDEEvaluator\",\"description\":\"DensityBasedAnomalyDetection\",\"features\":\"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete, setOwner, fsck\"}]}", "dedupeDef": "{\"alertDedupIntervalMin\":\"0\",\"emailDedupIntervalMin\":\"0\"}", "notificationDef": "", "remediationDef": "", "enabled": true } ]' 2>/dev/null)

echo "=> $result"

echo ""

echo "Creating alert executor for AlertExecutorService with userprofile-executor-definition.json"
result=$(curl -XPOST -H "Content-Type: application/json" \
        "http://$SERVICE_HOST:$SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertExecutorService" \
        -d '[ { "prefix": "alertExecutor", "tags":{ "site":"sandbox", "dataSource":"userProfile", "alertExecutorId" : "userProfileAnomalyDetectionExecutor", "streamName":"userActivity" }, "desc": "user activity data source" } ]' 2>/dev/null)

echo "=> $result"

echo ""

echo "Creating alert executor for AlertStreamService with userprofile-stream-description.json"
result=$(curl -XPOST -H "Content-Type: application/json" \
        "http://$SERVICE_HOST:$SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertStreamService" \
        -d '[ { "prefix": "alertStream", "tags": { "streamName": "userActivity", "site":"sandbox", "dataSource":"userProfile" }, "alertExecutorIdList": [ "userProfileAnomalyDetectionExecutor" ] } ]' 2>/dev/null)

echo "=> $result"