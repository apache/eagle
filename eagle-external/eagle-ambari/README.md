eagle-ambari-plugin
===================

Requirements
------------
1. Ambari `2.2.4` (has problems for higher version)

Installation
------------
1. Download certain version of eagle release package and install under to `/usr/hdp/current/eagle`
2. Execute ambari install

        ./bin/eagle-ambari.sh install

Uninstallation
------------
1. Uninstall services from amabri cluster

        ./bin/eagle-ambari.sh uninstall

Usage
-----
[https://wiki.vip.xyz.com/display/GDI/Eagle+in+Ambari](https://wiki.vip.xyz.com/display/GDI/Eagle+in+Ambari)

Reference
---------
* [Ambari: Create and Add the Service](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=38571133#Overview\(Ambari1.5.0orlater\)-CreateandAddtheService)
* [Quick links for custom services](https://issues.apache.org/jira/browse/AMBARI-11268)