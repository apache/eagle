---
layout: doc
title:  "Data Classification Tutorial" 
permalink: /docs/tutorial/classification-0.3.0.html
---

Apache Eagle data classification feature provides the ability to classify data with different levels of sensitivity.
Currently this feature is available ONLY for applications monitoring HDFS, Apache Hive and Apache HBase. For example, HdfsAuditLog, HiveQueryLog and HBaseSecurityLog. 

The main content of this page are 

* Connection Configuration
* Data Classification
 
 
### Connection Configuration

To monitor a remote cluster, we first make sure the connection to the cluster is configured. For more details, please refer to [Site Management](/docs/tutorial/site-0.3.0.html)

### Data Classification

After the configuration is The first part is about how to add/remove sensitivity to files/directories; the second part shows how to monitor these sensitive data. In the following, we take HdfsAuditLog as an example.

#### **Part 1: Sensitivity Edit**

  * add the sensitive mark to files/directories.

    * **Basic**: Label sensitivity files directly (**recommended**)

       ![HDFS classification](/images/docs/hdfs-mark1.png)
       ![HDFS classification](/images/docs/hdfs-mark2.png)
       ![HDFS classification](/images/docs/hdfs-mark3.png)
    * **Advanced**: Import json file/content

        ![HDFS classification](/images/docs/hdfs-import1.png)
        ![HDFS classification](/images/docs/hdfs-import2.png)
        ![HDFS classification](/images/docs/hdfs-import3.png)


 * remove sensitive mark on files/directories

   * **Basic**: remove label directly

        ![HDFS classification](/images/docs/hdfs-delete1.png)
        ![HDFS classification](/images/docs/hdfs-delete2.png)

   * **Advanced**: delete lin batch

        ![HDFS classification](/images/docs/hdfs-remove.png)

#### **Part 2: Sensitivity Usage in Policy Definition**

You can mark a particular folder/file as "PRIVATE". Once you have this information you can create policies using this label.

> For example: the following policy monitors all the operations to resources with sensitivity type "PRIVATE".

![sensitivity type policy](/images/docs/sensitivity-policy.png)

