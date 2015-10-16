/*
 Navicat MySQL Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50623
 Source Host           : localhost
 Source Database       : eagle

 Target Server Type    : MySQL
 Target Server Version : 50623
 File Encoding         : utf-8

 Date: 03/29/2015 01:49:47 AM
*/

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `unittest_testtsentity`
-- ----------------------------
DROP TABLE IF EXISTS `unittest_testtsentity`;
CREATE TABLE `unittest_testtsentity` (
  `uuid` varchar(255) NOT NULL,
  `field1` int(11) DEFAULT NULL,
  `field2` int(11) DEFAULT NULL,
  `field3` bigint(20) DEFAULT NULL,
  `field4` bigint(20) DEFAULT NULL,
  `field5` double DEFAULT NULL,
  `field6` double DEFAULT NULL,
  `field7` varchar(255) DEFAULT NULL,
  `cluster` varchar(255) DEFAULT NULL,
  `datacenter` varchar(255) DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `jobid` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `partition` (`cluster`,`datacenter`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;
