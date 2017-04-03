---
layout: doc
title:  "Site Management"
permalink: /docs/tutorial/site.html
---

Guide to site management for versions since **Apache Eagle 0.4.0-incubating**.

[For Apache Eagle 0.3.0-incubating, see _here_.](/docs/tutorial/site-0.3.0.html)

The main content in this page have two parts

* How to add a new site 
* How to add an new application

#### How to add a new site 

Eagle names the cluster to monitor as a site. Users (as an admin user) can manage their sites on Eagle UI. The following example is to add a new site 'test'.

![setup a site](/images/docs/new-site.png)

#### How to add an new application

Once create a site, users can choose the data sources they want to monitoring by adding different applications. For example, HdfsAuditLog application monitors the hdfs audit log. Currently, Eagle supports 

* [HDFS Data Activity Monitoring](/docs/hdfs-data-activity-monitoring.html)
* [HIVE Query Activity Monitoring](/docs/hive-query-activity-monitoring.html)
* [HBASE Data Activity Monitoring](/docs/hbase-data-activity-monitoring.html)
* [MapR FS Data Activity Monitoring](/docs/mapr-integration.html)
* [Hadoop JMX Metrics Monitoring](/docs/jmx-metric-monitoring.html)

Same as adding a site, application management also locates at the admin management page. To add an application, two steps are needed

#### step 1: go to site tab, and select the target site

![new application step1](/images/docs/new-application1.png)

#### step 2: click the applications you want on the right side and save the page

![new application step1](/images/docs/new-application2.png)
