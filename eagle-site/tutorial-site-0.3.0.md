---
layout: doc
title:  "Site Management"
permalink: /docs/tutorial/site-0.3.0.html
---

Apache Eagle (called Eagle in the following) identifies different Hadoop[^HADOOP] environments as different sites, such as sandbox, datacenter1, datacenter2. In each site, a user can add different data sources as the monitoring targets. For each data source, a connection configuration is required.

#### Step 1: Add Site

The following is an example which creates a new site "Demo", and add two data sources as its monitoring targets.
![setup a site](/images/docs/new-site.png)

#### Step 2: Add Configuration

After creating a new site, we need to edit the configuration to connect the cluster. 
Here we give configuration examples for HDFS, HBASE, and Hive. 

* HDFS

    ![hdfs setup](/images/docs/hdfs-setup-0.3.0.png) 
    
    * Base case

        You may configure the default path for Hadoop clients to connect remote hdfs namenode.

            {"fs.defaultFS":"hdfs://sandbox.hortonworks.com:8020"}

    * HA case

        Basically, you point your fs.defaultFS at your nameservice and let the client know how its configured (the backing namenodes) and how to fail over between them under the HA mode

            {"fs.defaultFS":"hdfs://nameservice1",
             "dfs.nameservices": "nameservice1",
             "dfs.ha.namenodes.nameservice1":"namenode1,namenode2",
             "dfs.namenode.rpc-address.nameservice1.namenode1": "hadoopnamenode01:8020",
             "dfs.namenode.rpc-address.nameservice1.namenode2": "hadoopnamenode02:8020",
             "dfs.client.failover.proxy.provider.nameservice1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            }

    * Kerberos-secured cluster

        For Kerberos-secured cluster, you need to get a keytab file and the principal from your admin, and configure "eagle.keytab.file" and "eagle.kerberos.principal" to authenticate its access.

            { "eagle.keytab.file":"/EAGLE-HOME/.keytab/eagle.keytab",
              "eagle.kerberos.principal":"eagle@SOMEWHERE.COM"
            }

        If there is an exception about "invalid server principal name", you may need to check the DNS resolver, or the data transfer , such as "dfs.encrypt.data.transfer", "dfs.encrypt.data.transfer.algorithm", "dfs.trustedchannel.resolver.class", "dfs.datatransfer.client.encrypt".
        
      

* Hive[^HIVE]
    * Basic

            {
              "accessType": "metastoredb_jdbc",
              "password": "hive",
              "user": "hive",
              "jdbcDriverClassName": "com.mysql.jdbc.Driver",
              "jdbcUrl": "jdbc:mysql://sandbox.hortonworks.com/hive?createDatabaseIfNotExist=true"
            }


* HBase[^HBASE]

    * Basic case

        You need to sett "hbase.zookeeper.quorum":"localhost" property and "hbase.zookeeper.property.clientPort" property.

            {
                "hbase.zookeeper.property.clientPort":"2181",
                "hbase.zookeeper.quorum":"localhost"
            }

    * Kerberos-secured cluster

        According to your environment, you can add or remove some of the following properties. Here is the reference.

            {
                "hbase.zookeeper.property.clientPort":"2181",
                "hbase.zookeeper.quorum":"localhost",
                "hbase.security.authentication":"kerberos",
                "hbase.master.kerberos.principal":"hadoop/_HOST@EXAMPLE.COM",
                "zookeeper.znode.parent":"/hbase",
                "eagle.keytab.file":"/EAGLE-HOME/.keytab/eagle.keytab",
                "eagle.kerberos.principal":"eagle@EXAMPLE.COM"
            }


#### Step 3: Checking the connection
After the configuration is ready, you can go to [classification page](/docs/tutorial/classification-0.3.0.html) and browse the data. If the configuration is correct, data will be ready in a few seconds.

Any questions on the Kerberos configuration in Eagle, please first check [FAQ](/docs/FAQ.html)


---

#### *Footnotes*

[^HADOOP]:*All mentions of "hadoop" on this page represent Apache Hadoop.*
[^HBASE]:*Apache HBase.*
[^HIVE]:*Apache Hive.*

