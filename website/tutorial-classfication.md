---
layout: doc
title:  "Data Classification Tutorial" 
permalink: /docs/tutorial/classification.html
---

Eagle Data classification provides the ability to classify HDFS and Hive data with different levels of sensitivity.
For both HDFS and Hive, a user can browse the resources and add/remove the sensitivity information.

The document has two parts. The first part is about how to add/remove sensitivity to files/directories; the second part shows the application
of sensitivity in policy definition. Showing HDFS as an example.

> **WARNING**: sensitivity is classified by sites. Please select the right site first when there are multiple ones.

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

