---
layout: doc
title:  "Apache Eagle Overview" 
permalink: /docs/index.html
---

## Some example


#### Apache Eagle Web Configuration


>- Sample about syntax highlight 

{% highlight batch %}
# eagle configuration
eagle{
  # eagle web service configuration
  service{
    # storage type: ["hbase","jdbc"]
    # default is "hbase"
    storage-type="hbase"

    # hbase configuration: hbase.zookeeper.quorum
    # default is "localhost"
    hbase-zookeeper-quorum="localhost"

    # hbase configuration: hbase.zookeeper.property.clientPort
    # default is 2181
    hbase-zookeeper-property-clientPort=2181

    # hbase configuration: zookeeper.znode.parent
    # default is "/hbase"
    zookeeper-znode-parent="/hbase-unsecure"
  }
}

{% endhighlight %}

how to make it?
[https://jekyllrb.com/docs/templates/](https://jekyllrb.com/docs/templates/)
