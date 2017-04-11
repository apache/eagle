<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Configuration
### System Level Configuration
Eagle system level configuration is typically configured in `conf/eagle.conf`.

    # ---------------------------------------------
    # Eagle REST Web Service Configuration
    # ---------------------------------------------
    service {
      env = "testing"
      host = "localhost"
      port = 9090
      username = "admin"
      password = "secret"
      readTimeOutSeconds = 60
      context = "/rest"
      timezone = "UTC"
    }

    zookeeper {
      zkQuorum = "localhost:2181"
      zkSessionTimeoutMs : 15000
      zkRetryTimes : 3
      zkRetryInterval : 20000
    }

    # ---------------------------------------------
    # Eagle Deep Storage Configuration
    # ---------------------------------------------

    storage {
      # storage type: ["hbase","jdbc"]
      # default is "hbase"
      type = "hbase"

      hbase {
        # hbase configuration: hbase.zookeeper.quorum
        # default is "localhost"
        zookeeperQuorum = "localhost"

        # hbase configuration: hbase.zookeeper.property.clientPort
        # default is 2181
        zookeeperPropertyClientPort = 2181

        # hbase configuration: zookeeper.znode.parent
        # default is "/hbase"
        zookeeperZnodeParent = "/hbase-unsecure"

        # eagle web login profile: [sandbox, default]
        # default is sandbox
        tableNamePrefixedWithEnvironment = false

        # eagle coprocessor enabled or not: [true, false]
        # default is false
        coprocessorEnabled = false
      }
    }

    # ---------------------------------------------
    # Eagle Metadata Store Configuration
    # ---------------------------------------------
    metadata {
      store = org.apache.eagle.metadata.service.memory.MemoryMetadataStore
      jdbc {
        username = "root"
        password = ""
        driverClassName = com.mysql.jdbc.Driver
        url = "jdbc:mysql://server.eagle.apache.org:3306/eagle"
      }
    }

    # ---------------------------------------------
    # Eagle Application Configuration
    # ---------------------------------------------
    application {
      sink {
        type = org.apache.eagle.app.sink.KafkaStreamSink
      }
      storm {
        nimbusHost = "server.eagle.apache.org"
        nimbusThriftPort = 6627
      }
      updateStatus: {
        initialDelay: 10
        period: 10
      }
    }

    # ---------------------------------------------
    # Eagle Alert Engine Configuration
    # ---------------------------------------------

    # Coordinator Configuration
    coordinator {
      policiesPerBolt = 5
      boltParallelism = 5
      policyDefaultParallelism = 5
      boltLoadUpbound = 0.8
      topologyLoadUpbound = 0.8
      numOfAlertBoltsPerTopology = 5
      zkConfig {
        zkQuorum = "server.eagle.apache.org:2181"
        zkRoot = "/alert"
        zkSessionTimeoutMs = 10000
        connectionTimeoutMs = 10000
        zkRetryTimes = 3
        zkRetryInterval = 3000
      }
      metadataService {
        host = "localhost",
        port = 9090,
        context = "/rest"
      }
      metadataDynamicCheck {
        initDelayMillis = 1000
        delayMillis = 30000
      }
    }



### Web Server Level Configuration
Web Server Level Configuration is typically configured in `conf/server.yaml`:

    server:
      applicationConnectors:
        - type: http
          port: 9090
      adminConnectors:
        - type: http
          port: 9091


    # ---------------------------------------------
    # Eagle Authentication Configuration
    # ---------------------------------------------
    auth:
      # indicating if authentication is enabled, true for enabled, false for disabled
      enabled: false

      # indicating authentication mode, "simple" or "ldap"
      mode: simple

      # indicating whether to use cache: cache is usually used for authentications that may
      # not handle high throughput (an RDBMS or LDAP server, for example)
      caching: false

      # indicating the cache policy, containing maximumSize and expireAfterWrite, e.g. maximumSize=10000, expireAfterWrite=10m
      cachePolicy: maximumSize=10000, expireAfterWrite=1m

      # indicating whether authorization is needed
      authorization: false

      # indicating whether @Auth annotation on parameters is needed
      annotated: true

      # for basic authentication, effective only when auth.mode=simple
      simple:
        # username for basic authentication, effective only when auth.mode=simple
        username: admin
        # password for basic authentication, effective only when auth.mode=simple
        password: secret

      # for ldap authentication, effective only when auth.mode=ldap
      ldap:
        uri: ldaps://ldap.server.address:636
        userFilter: ou=x,dc=y,dc=z
        groupFilter: ou=x,dc=y,dc=z
        userNameAttribute: cn
        groupNameAttribute: cn
        groupMembershipAttribute: memberUid
        groupClassName: posixGroup
        restrictToGroups:
          - user
          - admin
        connectTimeout: 500ms
        readTimeout: 500ms



As eagle server by default is based on DropWizard, so for more confgiruation details, please refer to [Dropwizard Configuration Reference](http://www.dropwizard.io/0.7.1/docs/manual/configuration.html)

### Application Level Configuration

Application level configurations could be set with `Settings` form during installation or modification. For more details of each application level configuration, please refer to ["Application"]("applications") guide pages.

---

# REST APIs
| Method        | Path         | Resource  |
| ------------- |--------------| ----------|
|DELETE  | /rest/metadata/clusters  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/clusters/{clusterId}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/datasources  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/datasources/{datasourceId}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/policies  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/policies/{policyId}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/publishmentTypes  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/publishmentTypes/{pubType}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/publishments  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/publishments/{name}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/streams  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/streams/{streamId}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/topologies  | org.apache.eagle.service.metadata.resource.MetadataResource|
|DELETE  | /rest/metadata/topologies/{topologyName}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/alerts  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/alerts/{alertId}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/assignments  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/clusters  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/datasources  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/policies  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/policies/{policyId}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/policies/{policyId}/publishments  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/publishmentTypes  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/publishments  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/schedulestates  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/schedulestates/{versionId}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/streams  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/metadata/topologies  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/alerts  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/alerts/batch  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/assignments  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/clear  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/clusters  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/clusters/batch  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/datasources  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/datasources/batch  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/export  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/import  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/policies  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/policies/batch  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/policies/parse  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/policies/validate  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/policies/{policyId}/publishments  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/policies/{policyId}/status/{status}  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/publishmentTypes  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/publishmentTypes/batch  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/publishments  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/publishments/batch  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/schedulestates  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/streams  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/streams/batch  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/topologies  | org.apache.eagle.service.metadata.resource.MetadataResource|
|POST    | /rest/metadata/topologies/batch  | org.apache.eagle.service.metadata.resource.MetadataResource|
|GET     | /rest/alert/topologies  | org.apache.eagle.service.topology.resource.TopologyMgmtResource|
|POST    | /rest/alert/topologies/{topologyName}/start  | org.apache.eagle.service.topology.resource.TopologyMgmtResource|
|POST    | /rest/alert/topologies/{topologyName}/stop  | org.apache.eagle.service.topology.resource.TopologyMgmtResource|
|GET     | /rest/coordinator/assignments  | org.apache.eagle.alert.coordinator.resource.CoordinatorResource|
|GET     | /rest/coordinator/periodicForceBuildState  | org.apache.eagle.alert.coordinator.resource.CoordinatorResource|
|POST    | /rest/coordinator/build  | org.apache.eagle.alert.coordinator.resource.CoordinatorResource|
|POST    | /rest/coordinator/disablePeriodicForceBuild  | org.apache.eagle.alert.coordinator.resource.CoordinatorResource|
|POST    | /rest/coordinator/enablePeriodicForceBuild  | org.apache.eagle.alert.coordinator.resource.CoordinatorResource|
|POST    | /rest/coordinator/refreshUsages  | org.apache.eagle.alert.coordinator.resource.CoordinatorResource|
|POST    | /rest/coordinator/validate  | org.apache.eagle.alert.coordinator.resource.CoordinatorResource|
|DELETE  | /rest/sites  | org.apache.eagle.metadata.resource.SiteResource|
|DELETE  | /rest/sites/{siteId}  | org.apache.eagle.metadata.resource.SiteResource|
|GET     | /rest/sites  | org.apache.eagle.metadata.resource.SiteResource|
|GET     | /rest/sites/{siteId}  | org.apache.eagle.metadata.resource.SiteResource|
|POST    | /rest/sites  | org.apache.eagle.metadata.resource.SiteResource|
|PUT     | /rest/sites  | org.apache.eagle.metadata.resource.SiteResource|
|PUT     | /rest/sites/{siteId}  | org.apache.eagle.metadata.resource.SiteResource|
|DELETE  | /rest/apps/uninstall  | org.apache.eagle.app.resource.ApplicationResource|
|GET     | /rest/apps  | org.apache.eagle.app.resource.ApplicationResource|
|GET     | /rest/apps/providers  | org.apache.eagle.app.resource.ApplicationResource|
|GET     | /rest/apps/providers/{type}  | org.apache.eagle.app.resource.ApplicationResource|
|GET     | /rest/apps/{appUuid}  | org.apache.eagle.app.resource.ApplicationResource|
|POST    | /rest/apps/install  | org.apache.eagle.app.resource.ApplicationResource|
|POST    | /rest/apps/start  | org.apache.eagle.app.resource.ApplicationResource|
|POST    | /rest/apps/status  | org.apache.eagle.app.resource.ApplicationResource|
|POST    | /rest/apps/stop  | org.apache.eagle.app.resource.ApplicationResource|
|POST    | /rest/apps/{appUuid}  | org.apache.eagle.app.resource.ApplicationResource|
|PUT     | /rest/apps/providers/reload  | org.apache.eagle.app.resource.ApplicationResource|
|GET     | /rest/example  | org.apache.eagle.app.example.extensions.ExampleResource|
|GET     | /rest/example/common  | org.apache.eagle.app.example.extensions.ExampleResource|
|GET     | /rest/example/config  | org.apache.eagle.app.example.extensions.ExampleResource|
|GET     | /rest/metadata/security/hbaseSensitivity  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|GET     | /rest/metadata/security/hdfsSensitivity  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|GET     | /rest/metadata/security/hiveSensitivity  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|GET     | /rest/metadata/security/ipzone  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|GET     | /rest/metadata/security/oozieSensitivity  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|POST    | /rest/metadata/security/hbaseSensitivity  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|POST    | /rest/metadata/security/hdfsSensitivity  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|POST    | /rest/metadata/security/hiveSensitivity  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|POST    | /rest/metadata/security/ipzone  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|POST    | /rest/metadata/security/oozieSensitivity  | org.apache.eagle.security.service.SecurityExternalMetadataResource|
|GET     | /rest/stream/attributeresolve  | org.apache.eagle.service.alert.resolver.AttributeResolveResource|
|POST    | /rest/stream/attributeresolve  | org.apache.eagle.service.alert.resolver.AttributeResolveResource|
|GET     | /rest/hbaseResource/columns  | org.apache.eagle.service.security.hbase.HbaseMetadataBrowseWebResource|
|GET     | /rest/hbaseResource/namespaces  | org.apache.eagle.service.security.hbase.HbaseMetadataBrowseWebResource|
|GET     | /rest/hbaseResource/tables  | org.apache.eagle.service.security.hbase.HbaseMetadataBrowseWebResource|
|GET     | /rest/oozieResource/coordinators  | org.apache.eagle.service.security.oozie.res.OozieMetadataBrowseWebResource|
|DELETE  | /rest/entities  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|GET     | /rest/entities  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|GET     | /rest/entities/jsonp  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|GET     | /rest/entities/rowkey  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|POST    | /rest/entities  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|POST    | /rest/entities  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|POST    | /rest/entities/delete  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|POST    | /rest/entities/rowkey  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|PUT     | /rest/entities  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|PUT     | /rest/entities  | org.apache.eagle.service.generic.GenericEntityServiceResource|
|GET     | /rest/list  | org.apache.eagle.service.generic.ListQueryResource|
|GET     | /rest/list/jsonp  | org.apache.eagle.service.generic.ListQueryResource|
|GET     | /rest/list/legacy  | org.apache.eagle.service.generic.ListQueryResource|
|GET     | /rest/meta  | org.apache.eagle.service.generic.MetadataResource|
|GET     | /rest/meta/resource  | org.apache.eagle.service.generic.MetadataResource|
|GET     | /rest/meta/service  | org.apache.eagle.service.generic.MetadataResource|
|POST    | /rest/metric  | org.apache.eagle.service.metric.EagleMetricResource|
|GET     | /rest/rowkeyquery  | org.apache.eagle.service.rowkey.RowKeyQueryResource|
|DELETE  | /rest/rowkey  | org.apache.eagle.service.rowkey.RowkeyResource|
|GET     | /rest/rowkey  | org.apache.eagle.service.rowkey.RowkeyResource|
|GET     | /rest/ValidateInternals  | org.apache.eagle.service.selfcheck.EagleServiceSelfCheckResource|
|GET     | /rest/services  | org.apache.eagle.service.selfcheck.ServiceResource|
|GET     | /rest/services/jsonp  | org.apache.eagle.service.selfcheck.ServiceResource|
|GET     | /rest/hdfsResource  | org.apache.eagle.service.security.hdfs.rest.HDFSResourceWebResource|
|GET     | /rest/mrJobs  | org.apache.eagle.service.jpm.MRJobExecutionResource|
|GET     | /rest/mrJobs/jobCountsByDuration  | org.apache.eagle.service.jpm.MRJobExecutionResource|
|GET     | /rest/mrJobs/jobMetrics/entities  | org.apache.eagle.service.jpm.MRJobExecutionResource|
|GET     | /rest/mrJobs/jobMetrics/list  | org.apache.eagle.service.jpm.MRJobExecutionResource|
|GET     | /rest/mrJobs/runningJobCounts  | org.apache.eagle.service.jpm.MRJobExecutionResource|
|GET     | /rest/mrJobs/search  | org.apache.eagle.service.jpm.MRJobExecutionResource|
|GET     | /rest/mrTasks/historyTaskCount  | org.apache.eagle.service.jpm.MRTaskExecutionResource|
|GET     | /rest/mrTasks/taskCountsByDuration  | org.apache.eagle.service.jpm.MRTaskExecutionResource|
|GET     | /rest/mrTasks/taskDistribution/{counterName}  | org.apache.eagle.service.jpm.MRTaskExecutionResource|
|GET     | /rest/mrTasks/taskSuggestion  | org.apache.eagle.service.jpm.MRTaskExecutionResource|
|GET     | /rest/swagger.{type:json or yaml}  | io.swagger.jaxrs.listing.ApiListingResource|
