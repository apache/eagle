---
layout: doc
title:  "Install Eagle" 
permalink: /docs/installation.html
---

### Install Eagle Sandbox

#### Pre-requisites

> To insall eagle on a sandbox you need to have orcale virtual box and HDP sandbox image.

1. [Oracle VirtualBox](https://www.virtualbox.org/wiki/Downloads).
2. [Hortonworks Sandbox](http://hortonworks.com/products/hortonworks-sandbox/#install) v 2.2.4 or later.

#### Register HDP sandbox 

1. [Register](http://127.0.0.1:8888/) Hortonworks sandbox.
2. [Enable Ambari](http://127.0.0.1:8000/). Click on Enable Button.
3. [Login](http://127.0.0.1:8080) as admin/admin.

#### Install Eagle

* **Step 1**: Clone stable version from [eagle github](https://github.corp.ebay.com/eagle/eagle/tree/release1.0)
>       Build project mvn clean install -DskipTests=true

* **Step 2**:  Download eagle-bin-0.1.0.tar.gz package from successful build into your HDP sandbox.

    * Option 1: `scp -P 2222  eagle/eagle-assembly/target/eagle-0.1.0-bin.tar.gz root@127.0.0.1:/usr/hdp/current/`


    * Option 2: Create shared directory between host and Sandbox, and restart Sandbox. Then you can find the shared directory under /media in Sandbox.

		![Adding a shared folder](/images/docs/Sharedfolder.jpg "Adding a shared folder")

* **Step 3**: Extract eagle tarball package

      $ cd /usr/hdp/current
      $ tar -zxvf eagle-0.1.0-bin.tar.gz
      $ mv eagle-0.1.0 eagle

* **Step 4**: Add root as a HBase superuser via [Ambari](http://127.0.0.1:8080/#/main/services/HBASE/configs) (Optional, a user can operate HBase by sudo su hbase, as an alternative).

* **Step 5**: Install Eagle Ambari service 
>
    /usr/hdp/current/eagle/bin/eagle-ambari.sh install.

* **Step 6**: Restart [Ambari](http://127.0.0.1:8000/) click on disable and enable Ambari back.

* **Step 7**: Start HBase & Storm & Kafka
From Ambari UI, restart any suggested components("Restart button on top") & Start Storm (Start "Nimbus" ,"Supervisor" & "Storm UI Server"), Kafka (Start "Kafka Broker") , HBase (Start "RegionServer"  and " HBase Master") 
>
![Restart Services](/images/docs/Services.png "Services")

* **Step 8**: Add Eagle Service To Ambari. (Click For Video)

	* Click on "Add Service" under Actions button on Ambari Main page 

		![AddService](/images/docs/AddService.png "AddService")
	
	* Select "Eagle" in list of services and proceed to install all eagle services. 
EagleServiceSuccess

		![Eagle Services](/images/docs/EagleServiceSuccess.png "Eagle Services")

* **Step 9**: Add Policies and meta data required by running below script.

      $ /usr/hdp/current/eagle/examples/sample-sensitivity-resource-create.sh 
      $ /usr/hdp/current/eagle/examples/sample-policy-create.sh

