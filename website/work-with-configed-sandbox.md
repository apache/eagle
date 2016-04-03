---
layout: doc
title:  "Work with configed Sandbox"
permalink: /docs/work-with-configed-sandbox.html
---

After Eagle has been deployed into the sandbox and all configuration has been done, you should have no issue to have a running eagle on your machine. But if you restart the sandbox, following steps have to be done to let you have the running eagle again:

    * You should use a web browser and login into Ambari and start HBase, Kafka, and Storm there. Please wait until all of them are running. It will take some time.
    * login into the sandbox via terminal and run $ /usr/hdp/current/eagle/bin/eagle-service start.
    * You can use e.g. $ps au | grep eagle to check if the eagle service is running.

