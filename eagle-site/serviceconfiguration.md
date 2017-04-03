---
layout: doc
title:  "Apache Eagle Service Configuration"
permalink: /docs/serviceconfiguration.html
---

Apache Eagle (called Eagle in the following) Service provides some config files for specifying metadata storage, security access to Eagle Service. This page will give detailed
description of Eagle Service configuration.

Eagle currently supports to customize the following configurations:

* Metadata store config
* Security access config

### Metadata store config 
* for hbase

~~~
eagle {
	service{
		storage-type="hbase"
		hbase-zookeeper-quorum="sandbox.hortonworks.com"
		hbase-zookeeper-property-clientPort=2181
		zookeeper-znode-parent="/hbase-unsecure",
		springActiveProfile="sandbox"
		audit-enabled=true
	}
      }
~~~

* for mysql

~~~
eagle {
	service {
		storage-type="jdbc"
		storage-adapter="mysql"
		storage-username="eagle"
		storage-password=eagle
		storage-database=eagle
		storage-connection-url="jdbc:mysql://localhost:3306/eagle"
		storage-connection-props="encoding=UTF-8"
		storage-driver-class="com.mysql.jdbc.Driver"
		storage-connection-max=8
	}
}
~~~

* for derby

~~~
eagle {
	service {
		storage-type="jdbc"
		storage-adapter="derby"
		storage-username="eagle"
		storage-password=eagle
		storage-database=eagle
		storage-connection-url="jdbc:derby:/tmp/eagle-db-dev;create=true"
		storage-connection-props="encoding=UTF-8"
		storage-driver-class="org.apache.derby.jdbc.EmbeddedDriver"
		storage-connection-max=8
	}
}
~~~
<br />
