---
layout: doc
title:  "Setup Development Env" 
permalink: /docs/development.html
---

This guide elaborates how to develop Eagle using IDE and Hadoop sandbox in mac OSX. We use Intellij and HDP sandbox as examples.

### Setup HDP sandbox

Download Hortonworks Sandbox with HDP 2.2.4.2 through [http://hortonworks.com/products/hortonworks-sandbox/#archive](http://hortonworks.com/products/hortonworks-sandbox/#archive)

Make sure HBASE master/regionserver are started through ambari UI, http://yoursandbox:8080

### Environment on the dev machine

#### Install maven

The latest maven can be found at [http://maven.apache.org/download.cgi](http://maven.apache.org/download.cgi), we add maven bin into PATH so that mvn can be run everywhere.

    # cd ~
    wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
    tar -xzvf apache-maven-3.2.5-bin.tar.gz
    export PATH=/Users/yourusername/apache-maven-3.2.5/bin:$PATH

#### Compile


## start Eagle service in Intellij

## start storm topology in Intellij

 

