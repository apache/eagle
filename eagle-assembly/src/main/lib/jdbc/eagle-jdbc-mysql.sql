-- MySQL dump 10.13  Distrib 5.6.23, for osx10.8 (x86_64)
--
-- Host: localhost    Database: eagle
-- ------------------------------------------------------
-- Server version	5.6.23

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `alertdef_alertdef`
--

DROP TABLE IF EXISTS `alertdef_alertdef`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alertdef_alertdef` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `site` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `application` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `alertexecutorid` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `policyid` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `policytype` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(200) COLLATE utf8_bin DEFAULT NULL,
  `policydef` varchar(1000) COLLATE utf8_bin DEFAULT NULL,
  `dedupedef` varchar(1000) COLLATE utf8_bin DEFAULT NULL,
  `notificationdef` varchar(1000) COLLATE utf8_bin DEFAULT NULL,
  `remediationdef` varchar(1000) COLLATE utf8_bin DEFAULT NULL,
  `enabled` tinyint(1) DEFAULT NULL,
  `owner` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `lastmodifieddate` bigint(20) DEFAULT NULL,
  `severity` bigint(20) DEFAULT NULL,
  `createdtime` bigint(20) DEFAULT NULL,
  `markdownReason` varchar(1000) COLLATE utf8_bin DEFAULT NULL,
  `markdownEnabled` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `alertdef_alertdef`
--

LOCK TABLES `alertdef_alertdef` WRITE;
/*!40000 ALTER TABLE `alertdef_alertdef` DISABLE KEYS */;
/*!40000 ALTER TABLE `alertdef_alertdef` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `alertdetail_hadoop`
--

DROP TABLE IF EXISTS `alertdetail_hadoop`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alertdetail_hadoop` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `site` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `application` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `hostname` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `policyid` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `alertsource` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `sourcestreams` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `alertexecutorid` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `remediationid` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `remediationcallback` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `alertcontext` varchar(1000) COLLATE utf8_bin DEFAULT NULL,
  `streamid` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `alertdetail_hadoop`
--

LOCK TABLES `alertdetail_hadoop` WRITE;
/*!40000 ALTER TABLE `alertdetail_hadoop` DISABLE KEYS */;
/*!40000 ALTER TABLE `alertdetail_hadoop` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `alertexecutor_alertexecutor`
--

DROP TABLE IF EXISTS `alertexecutor_alertexecutor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alertexecutor_alertexecutor` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `application` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `alertexecutorid` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `streamname` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `site` varchar(45) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `alertexecutor_alertexecutor`
--

LOCK TABLES `alertexecutor_alertexecutor` WRITE;
/*!40000 ALTER TABLE `alertexecutor_alertexecutor` DISABLE KEYS */;
INSERT INTO `alertexecutor_alertexecutor` VALUES ('0ijKT3_____62aP_uMZ-K1SsoVDrH3vKa382HVykBVAJItDs',0,'hiveQueryLog','hiveAccessAlertByRunningJob','hiveAccessLogStream','alert executor for hive query log event stream',NULL),('0ijKT3_____62aP_uMZ-K2-GR_rrH3vKDuSvMwA130dvL77HXKQFUNuQa14',0,'userProfile','userProfileAnomalyDetectionExecutor','userActivity','user activity data source','sandbox'),('0ijKT3_____62aP_uMZ-K4uAAAjrH3vKhK_cHVykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogAlertExecutor','hdfsAuditLogEventStream','alert executor for hdfs audit log event stream',NULL),('0ijKT3_____62aP_uMZ-K_85ls_rH3vK8F7dJFykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogAlertExecutor','hbaseSecurityLogEventStream','alert executor for hbase security log event stream',NULL);
/*!40000 ALTER TABLE `alertexecutor_alertexecutor` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `alertstream_alertstream`
--

DROP TABLE IF EXISTS `alertstream_alertstream`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alertstream_alertstream` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `application` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `streamname` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `site` varchar(45) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `alertstream_alertstream`
--

LOCK TABLES `alertstream_alertstream` WRITE;
/*!40000 ALTER TABLE `alertstream_alertstream` DISABLE KEYS */;
INSERT INTO `alertstream_alertstream` VALUES ('x3ZP_H_____62aP_uMZ-K2-GR_oANd9Hby--x1ykBVDbkGte',0,'userProfile','userActivity',NULL,'sandbox');
/*!40000 ALTER TABLE `alertstream_alertstream` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `alertstreamschema_alertstreamschema`
--

DROP TABLE IF EXISTS `alertstreamschema_alertstreamschema`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alertstreamschema_alertstreamschema` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `application` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `streamname` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `attrname` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `attrtype` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `category` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `attrValueResolver` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `usedastag` tinyint(1) DEFAULT NULL,
  `attrdescription` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `attrdisplayname` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `defaultvalue` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `alertstreamschema_alertstreamschema`
--

LOCK TABLES `alertstreamschema_alertstreamschema` WRITE;
/*!40000 ALTER TABLE `alertstreamschema_alertstreamschema` DISABLE KEYS */;
INSERT INTO `alertstreamschema_alertstreamschema` VALUES ('iSeEvX_____62aP_uMZ-K1SsoVAhAmgc66vEDlykBVAJItDs',0,'hiveQueryLog','hiveAccessLogStream','resource','string','','org.apache.eagle.service.security.hive.resolver.HiveMetadataResolver',NULL,'/database/table/column or /database/table/*',NULL,NULL),('iSeEvX_____62aP_uMZ-K1SsoVAhAmgcA0kpFlykBVAJItDs',0,'hiveQueryLog','hiveAccessLogStream','timestamp','long','','',NULL,'milliseconds of the datetime',NULL,NULL),('iSeEvX_____62aP_uMZ-K1SsoVAhAmgcADbry1ykBVAJItDs',0,'hiveQueryLog','hiveAccessLogStream','user','string','','',NULL,'process user',NULL,NULL),('iSeEvX_____62aP_uMZ-K1SsoVAhAmgcOKXfS1ykBVAJItDs',0,'hiveQueryLog','hiveAccessLogStream','command','string','','org.apache.eagle.service.security.hive.resolver.HiveCommandResolver',NULL,'hive sql command, such as SELECT, INSERT and DELETE',NULL,NULL),('iSeEvX_____62aP_uMZ-K1SsoVAhAmgcX026eVykBVAJItDs',0,'hiveQueryLog','hiveAccessLogStream','sensitivityType','string','','org.apache.eagle.service.security.hive.resolver.HiveSensitivityTypeResolver',NULL,'mark such as PHONE_NUMBER',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcA0kpFlykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','timestamp','long','','',NULL,'milliseconds of the datetime',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcAAG95FykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','src','string','','org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver',NULL,'source directory or file, such as /tmp',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcAAGBOlykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','cmd','string','','org.apache.eagle.service.security.hdfs.resolver.HDFSCommandResolver',NULL,'file/directory operation, such as getfileinfo, open, listStatus and so on',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcAAGFxVykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','dst','string','','org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver',NULL,'destination directory, such as /tmp',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcADD1qFykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','host','string','','',NULL,'hostname, such as localhost',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcADbry1ykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','user','string','','',NULL,'process user',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcMC9vDFykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','securityZone','string','','',NULL,'',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcX026eVykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','sensitivityType','string','','org.apache.eagle.service.security.hdfs.resolver.HDFSSensitivityTypeResolver',NULL,'mark such as AUDITLOG, SECURITYLOG',NULL,NULL),('iSeEvX_____62aP_uMZ-K4uAAAghAmgcya4BqFykBVB3iZNS',0,'hdfsAuditLog','hdfsAuditLogEventStream','allowed','bool','','',NULL,'true, false or none',NULL,NULL),('iSeEvX_____62aP_uMZ-K_85ls8hAmgcA0kpFlykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogEventStream','timestamp','long','','',NULL,'milliseconds of the datetime',NULL,NULL),('iSeEvX_____62aP_uMZ-K_85ls8hAmgcADD1qFykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogEventStream','host','string','','',NULL,'remote ip address to access hbase',NULL,NULL),('iSeEvX_____62aP_uMZ-K_85ls8hAmgcADbry1ykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogEventStream','user','string','','',NULL,'hbase user',NULL,NULL),('iSeEvX_____62aP_uMZ-K_85ls8hAmgcBoM-VFykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogEventStream','scope','string','','org.apache.eagle.service.security.hbase.resolver.HbaseMetadataResolver',NULL,'the resources which users are then granted specific permissions (Read, Write, Execute, Create, Admin) against',NULL,NULL),('iSeEvX_____62aP_uMZ-K_85ls8hAmgcQU7yj1ykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogEventStream','request','string','','org.apache.eagle.service.security.hbase.resolver.HbaseRequestResolver',NULL,'',NULL,NULL),('iSeEvX_____62aP_uMZ-K_85ls8hAmgcX026eVykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogEventStream','sensitivityType','string','','org.apache.eagle.service.security.hbase.resolver.HbaseSensitivityTypeResolver',NULL,'',NULL,NULL),('iSeEvX_____62aP_uMZ-K_85ls8hAmgcqy9-NlykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogEventStream','action','string','','org.apache.eagle.service.security.hbase.resolver.HbaseActionResolver',NULL,'action types, such as read, write, create, execute, and admin',NULL,NULL),('iSeEvX_____62aP_uMZ-K_85ls8hAmgcys3P8lykBVCMCnLr',0,'hbaseSecurityLog','hbaseSecurityLogEventStream','status','string','','',NULL,'access status: allowed or denied',NULL,NULL);
/*!40000 ALTER TABLE `alertstreamschema_alertstreamschema` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `eagleapplicationdesc_eagleapplicationdesc`
--

DROP TABLE IF EXISTS `eagleapplicationdesc_eagleapplicationdesc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `eagleapplicationdesc_eagleapplicationdesc` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `application` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `alias` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `groupName` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `features` blob,
  `config` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `eagleapplicationdesc_eagleapplicationdesc`
--

LOCK TABLES `eagleapplicationdesc_eagleapplicationdesc` WRITE;
/*!40000 ALTER TABLE `eagleapplicationdesc_eagleapplicationdesc` DISABLE KEYS */;
INSERT INTO `eagleapplicationdesc_eagleapplicationdesc` VALUES ('54TRXX_____62aP_XKQFUAki0Ow',0,'hiveQueryLog','Hive query log security check application','HIVE','DAM','\0\0\0\0\0\0\0\0\0common\0\0\0\0\0\0classification\0\0\0\0\0\0userProfile\0\0\0\0\0\0metadata','{\n	\"view\": {\n		\"prefix\": \"hiveResourceSensitivity\",\n		\"service\": \"HiveResourceSensitivityService\",\n		\"keys\": [\n			\"hiveResource\",\n			\"sensitivityType\"\n		],\n		\"type\": \"table\",\n		\"api\": {\n			\"database\": \"hiveResource/databases\",\n			\"table\": \"hiveResource/tables\",\n			\"column\": \"hiveResource/columns\"\n		},\n		\"mapping\": {\n			\"database\": \"database\",\n			\"table\": \"table\",\n			\"column\": \"column\"\n		}\n	}\n}'),('54TRXX_____62aP_XKQFUHeJk1I',0,'hdfsAuditLog','HDFS audit log security check application','HDFS','DAM','\0\0\0\0\0\0\0\0\0common\0\0\0\0\0\0classification\0\0\0\0\0\0userProfile\0\0\0\0\0\0metadata','{\n	\"view\": {\n		\"prefix\": \"fileSensitivity\",\n		\"service\": \"FileSensitivityService\",\n		\"keys\": [\n			\"filedir\",\n			\"sensitivityType\"\n		],\n		\"type\": \"folder\",\n		\"api\": \"hdfsResource\"\n	}\n}'),('54TRXX_____62aP_XKQFUIwKcus',0,'hbaseSecurityLog','HBASE audit log security check application','HBASE','DAM','\0\0\0\0\0\0\0\0\0common\0\0\0\0\0\0classification\0\0\0\0\0\0userProfile\0\0\0\0\0\0metadata','{\n	\"view\": {\n		\"prefix\": \"hbaseResourceSensitivity\",\n		\"service\": \"HbaseResourceSensitivityService\",\n		\"keys\": [\n			\"hbaseResource\",\n			\"sensitivityType\"\n		],\n		\"type\": \"table\",\n		\"api\": {\n			\"database\": \"hbaseResource/namespaces\",\n			\"table\": \"hbaseResource/tables\",\n			\"column\": \"hbaseResource/columns\"\n		},\n		\"mapping\": {\n			\"database\": \"namespace\",\n			\"table\": \"table\",\n			\"column\": \"columnFamily\"\n		}\n	}\n}');
/*!40000 ALTER TABLE `eagleapplicationdesc_eagleapplicationdesc` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `eaglefeaturedesc_eaglefeaturedesc`
--

DROP TABLE IF EXISTS `eaglefeaturedesc_eaglefeaturedesc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `eaglefeaturedesc_eaglefeaturedesc` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `feature` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `version` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `eaglefeaturedesc_eaglefeaturedesc`
--

LOCK TABLES `eaglefeaturedesc_eaglefeaturedesc` WRITE;
/*!40000 ALTER TABLE `eaglefeaturedesc_eaglefeaturedesc` DISABLE KEYS */;
INSERT INTO `eaglefeaturedesc_eaglefeaturedesc` VALUES ('4DMSA3_____62aP_xaJ69hbKM-Y',0,'classification','Sensitivity browser of the data classification.','v0.3.0'),('4DMSA3_____62aP_xaJ69jj4wMM',0,'metrics','Metrics dashboard','v0.3.0'),('4DMSA3_____62aP_xaJ69q8_Kes',0,'common','Provide the Policy & Alert feature.','v0.3.0'),('4DMSA3_____62aP_xaJ69tuQa14',0,'userProfile','Machine learning of the user profile','v0.3.0'),('4DMSA3_____62aP_xaJ69uUtey8',0,'metadata','Stream metadata viewer','v0.3.0');
/*!40000 ALTER TABLE `eaglefeaturedesc_eaglefeaturedesc` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `eaglesiteapplication_eaglesiteapplication`
--

DROP TABLE IF EXISTS `eaglesiteapplication_eaglesiteapplication`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `eaglesiteapplication_eaglesiteapplication` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `site` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `application` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `enabled` tinyint(1) DEFAULT NULL,
  `config` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `eaglesiteapplication_eaglesiteapplication`
--

LOCK TABLES `eaglesiteapplication_eaglesiteapplication` WRITE;
/*!40000 ALTER TABLE `eaglesiteapplication_eaglesiteapplication` DISABLE KEYS */;
INSERT INTO `eaglesiteapplication_eaglesiteapplication` VALUES ('D-7M5X_____62aP_ADXfR28vvsdcpAVQCSLQ7A',0,'sandbox','hiveQueryLog',1,'{\"accessType\":\"metastoredb_jdbc\",\"password\":\"hive\",\"user\":\"hive\",\"jdbcDriverClassName\":\"com.mysql.jdbc.Driver\",\"jdbcUrl\":\"jdbc:mysql://sandbox.hortonworks.com/hive?createDatabaseIfNotExist=true\"}'),('D-7M5X_____62aP_ADXfR28vvsdcpAVQd4mTUg',0,'sandbox','hdfsAuditLog',1,'{\"fs.defaultFS\":\"hdfs://sandbox.hortonworks.com:8020\"}'),('D-7M5X_____62aP_ADXfR28vvsdcpAVQjApy6w',0,'sandbox','hbaseSecurityLog',1,'{\"hbase.zookeeper.property.clientPort\":\"2181\", \"hbase.zookeeper.quorum\":\"localhost\"}');
/*!40000 ALTER TABLE `eaglesiteapplication_eaglesiteapplication` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `eaglesitedesc_eaglesitedesc`
--

DROP TABLE IF EXISTS `eaglesitedesc_eaglesitedesc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `eaglesitedesc_eaglesitedesc` (
  `uuid` varchar(100) COLLATE utf8_bin NOT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `site` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `enabled` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `eaglesitedesc_eaglesitedesc`
--

LOCK TABLES `eaglesitedesc_eaglesitedesc` WRITE;
/*!40000 ALTER TABLE `eaglesitedesc_eaglesitedesc` DISABLE KEYS */;
INSERT INTO `eaglesitedesc_eaglesitedesc` VALUES ('phJknH_____62aP_ADXfR28vvsc',0,'sandbox',1);
/*!40000 ALTER TABLE `eaglesitedesc_eaglesitedesc` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-03-08 18:34:19
