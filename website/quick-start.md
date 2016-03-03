---
layout: doc
title:  "Quick Start" 
permalink: /docs/quick-start.html
---

This is a tutorial-style guide for users to have a quick image of Eagle. The main content are

* Downloading
* Installation
* Demos

### Download/Build tarball

* Download tarball directly from latest released [binary package](http://66.211.190.194/eagle-0.1.0.tar.gz)

* Build manually by cloning latest code from [github](https://github.com/apache/incubator-eagle) with [Apache Maven](https://maven.apache.org/):

	  $ git clone git@github.com:apache/incubator-eagle.git
	  $ cd Eagle
	  $ mvn clean package -DskipTests

	After building successfully, you will get the tarball under `eagle-assembly/target/` named as `eagle-${version}-bin.tar.gz`
<br/>

### Installation
The fastest way to start with Eagle is to:

* [Install Eagle with Sandbox](/docs/deployment-in-sandbox.html)
* [Install Eagle with Docker](https://issues.apache.org/jira/browse/EAGLE-3)(under development)

If you want to deploy eagle in production environment, please refer to:

* [Deploy Eagle in the Production](/docs/deployment-in-production.html)
<br/>

### Demos

* Define policy with Eagle web
    * Step 1: Select the site which is monitored by the backend topologies. For example "sandbox"
        ![](/images/docs/selectSite.png)
    * Step 2: Create a policy
        ![](/images/docs/hdfs-policy1.png)

    Learn more about how to define policy, please refer to tutorial [Policy Management](/docs/tutorial/policy.html)
<br/>

* Test policy and check alerting

    **Example 1** (HDFSAuditLog): validate sample policy “viewPrivate” on [Eagle web](http://localhost:9099/eagle-service) by running a HDFS command

      $ hdfs dfs -cat /tmp/private

    You should see an alert for policy name “viewPrivate” in [Eagle web](http://localhost:9099/eagle-service) . Under Alerts page.

    **Example 2** (HiveQueryLog): validate sample policy “queryPhoneNumber” in [Eagle web](http://localhost:9099/eagle-service) by submitting a hive job

      $ su hive
      $ hive
      > set hive.execution.engine=mr;
      > use xademo;
      > select a.phone_number from customer_details a, call_detail_records b where a.phone_number=b.phone_number;

  You should see an alert for policy name “queryPhoneNumber” in [Eagle web](http://localhost:9099/eagle-service) . Under Alerts page.

<br/>

<br/>
