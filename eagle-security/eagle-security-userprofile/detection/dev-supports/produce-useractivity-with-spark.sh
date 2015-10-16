#!/usr/bin/env bash

spark-submit --master yarn-cluster --deploy-mode cluster --class eagle.security.userprofile.UserProfileTrainingCLI eagle-security-userprofile-training-0.1.0-assembly.jar --master yarn-cluster --period PT1M --input /tmp/auditlog/* --kafka-props topic=sandbox_hdfs_audit_agg,metadata.broker.list=sandbox.hortonworks.com:6667