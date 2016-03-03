---
layout: doc
title:  "Eagle LDAP Tutorial"
permalink: /docs/tutorial/ldap.html
---

To enable Eagle LDAP authentication on the web, two steps are needed.

Step 1: edit configuration under lib/tomcat/webapps/eagle-service/WEB-INF/classes/ldap.properties.

    ldap.server=ldap://localhost:10389
    ldap.username=uid=admin,ou=system
    ldap.password=secret
    ldap.user.searchBase=ou=Users,o=mojo
    ldap.user.searchPattern=(uid={0})
    ldap.user.groupSearchBase=ou=groups,o=mojo
    acl.adminRole=
    acl.defaultRole=ROLE_USER

acl.adminRole and acl.defaultRole are two custom properties for Eagle. Eagle manages admin users with groups. If you set acl.adminRole as ROLE_{EAGLE-ADMIN-GROUP-NAME}, members in this group have admin privilege. acl.defaultRole is ROLE_USER.

Step 2: edit conf/eagle-service.conf, and add springActiveProfile="default"

    eagle{
        service{
            storage-type="hbase"
            hbase-zookeeper-quorum="localhost"
            hbase-zookeeper-property-clientPort=2181
            zookeeper-znode-parent="/hbase",
            springActiveProfile="default"
        }
    }






