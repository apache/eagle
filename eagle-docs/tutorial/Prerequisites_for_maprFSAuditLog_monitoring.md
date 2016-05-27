<!--
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
-->

### Prerequisites

To get maprFSAuditLog monitoring started, we need to:

* Enable audit logs on MapR from MapR's terminal
* Created logstash conf file to send audit logs to kafka
* Initialize metadata for mapFSAuditLog and enabled the application

Here are the steps to follow:   

#### Step1: Enable audit logs for FileSystem Operations and Table Operations in MapR
First we need to enable data auditing at all three levels: cluster level, volume level and directory,file or table level. 
##### Cluster level: 

```      
       $ maprcli audit data -cluster <cluster name> -enabled true 
                           [ -maxsize <GB, defaut value is 32. When size of audit logs exceed this number, an alarm will be sent to the dashboard in the MapR Control Service > ]
                           [ -retention <number of Days> ]
```
Example:

```
        $ maprcli audit data -cluster mapr.cluster.com -enabled true -maxsize 30 -retention 30
```



##### Volume level:

```      
       $ maprcli volume audit -cluster <cluster name> -enabled true 
                            -name <volume name>
                            [ -coalesce <interval in minutes, the interval of time during which READ, WRITE, or GETATTR operations on one file from one client IP address are logged only once, if auditing is enabled> ]
```
Example:

```
        $ maprcli volume audit -cluster mapr.cluster.com -name mapr.tmp -enabled true
```

To verify that auditing is enabled for a particular volume, use this command:

```
        $ maprcli volume info -name <volume name> -json | grep -i 'audited\|coalesce'
```
and you should see something like this:

```
                        "audited":1,
                        "coalesceInterval":60
```
If "audited" is '1' then auditing is enabled for this volume.



##### Directory, file, or MapR-DB table level:

```
        $ hadoop mfs -setaudit on <directory|file|table>
```

To check whether Auditing is Enabled for a Directory, File, or MapR-DB Table, use ``$ hadoop mfs -ls``
Example:
Before enable the audit log on file ``/tmp/dir``, try ``$ hadoop mfs -ls /tmp/dir``, you should see something like this:
```
drwxr-xr-x Z U U   - root root          0 2016-03-02 15:02  268435456 /tmp/dir
               p 2050.32.131328  mapr2.da.dg:5660 mapr1.da.dg:5660
```
The second ``U`` means auditing on this file is not enabled. 
Enable auditing with this command: 
```
$ hadoop mfs -setaudit on /tmp/dir
```
Then check the auditing bit with : 
```
$ hadoop mfs -ls /tmp/dir
```
you should see something like this:
```
drwxr-xr-x Z U A   - root root          0 2016-03-02 15:02  268435456 /tmp/dir
               p 2050.32.131328  mapr2.da.dg:5660 mapr1.da.dg:5660
```
We can see the previous ``U`` has been changed to ``A`` which indicates auditing on this file is enabled.
  
###### Important:
When a directory has been enabled auditing,  directories/files located in this dir won't inherit auditing, but a newly created file/dir (after enabling the auditing on this dir) in this directory will.



#### Step2: Stream log data into Kafka by using Logstash
As MapR do not have name node, instead it use CLDB service, we have to use logstash to stream log data into kafka.
- First find out the nodes that have CLDB service
- Then find out the location of audit log files, eg: ``/mapr/mapr.cluster.com/var/mapr/local/mapr1.da.dg/audit/``, file names should be in this format: ``FSAudit.log-2016-05-04-001.json`` 
- Created a logstash conf file and run it, following this doc[Logstash-kafka](https://github.com/apache/incubator-eagle/blob/dev/eagle-assembly/src/main/docs/logstash-kafka-conf.md)


#### Step3: Set up maprFSAuditLog applicaiton in Eagle Service
After Eagle Service gets started, create mapFSAuditLog application using:  ``$ ./maprFSAuditLog-init.sh``. By default it will create maprFSAuditLog in site "sandbox", you may need to change it to your own site.
After these steps you are good to go.

Have fun!!! :)

### Reference Links
1. [Enable Auditing in MapR](http://doc.mapr.com/display/MapR/Enabling+Auditing)
2. [MapR audit logs](http://doc.mapr.com/display/MapR/Audit+Logs+for+Filesystem+Operations+and+Table+Operations)