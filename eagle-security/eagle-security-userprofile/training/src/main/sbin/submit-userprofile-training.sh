#!/usr/bin/env bash

cd $(dirname $0)/../

./bin/submit-userprofile-training.sh --master yarn-cluster \
									--input /logs/auditlog/* \
									--service-host localhost \
									--service-port 9099
