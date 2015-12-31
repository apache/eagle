<!--
{% comment %}
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
{% endcomment %}
-->

# Apache Eagle [![Build Status](https://builds.apache.org/buildStatus/icon?job=incubator-eagle-pr-reviewer)](https://builds.apache.org/job/incubator-eagle-pr-reviewer/)

>  Secure Hadoop Data in Real Time

Apache Eagle is an open source monitoring solution to instantly identify access to sensitive data, recognize attacks, malicious activities in Hadoop and take actions in real time.

For more details, please visit [https://eagle.incubator.apache.org](https://eagle.incubator.apache.org)

## Documentation
You can find the latest Eagle documentation on [https://eagle.incubator.apache.org](https://eagle.incubator.apache.org/docs). This [README](README) file only contains basic setup instructions.

## Getting Started
The fastest way to get started with Eagle is to run with [docker](https://github.com/docker/docker) by one of following options:

* Pull latest eagle docker image from [docker hub](https://hub.docker.com/r/apacheeagle/sandbox/) directly:

        docker pull apacheeagle/sandbox

  Then run eagle docker image:
  
      docker run -p 9099:9099 -p 8080:8080 -p 8744:8744 -p 2181:2181 -p 2888:2888 -p 6667:6667 -p 60020:60020 \
        -p 60030:60030 -p 60010:60010 -d --dns 127.0.0.1 --entrypoint /usr/local/serf/bin/start-serf-agent.sh \
        -e KEYCHAIN= --env EAGLE_SERVER_HOST=sandbox.eagle.incubator.apache.org --name sandbox \
        -h sandbox.eagle.incubator.apache.org --privileged=true apacheeagle/sandbox:latest \
        --tag ambari-server=true
      docker run -it --rm -e EXPECTED_HOST_COUNT=1 -e BLUEPRINT=hdp-singlenode-eagle --link sandbox:ambariserver\
        --entrypoint /bin/sh apacheeagle/sandbox:latest -c /tmp/install-cluster.sh

* Build eagle docker image from source code with [eagle-docker](eagle-external/eagle-docker) tool.

         git clone https://github.com/apache/incubator-eagle.git
         cd incubator-eagle && ./eagle-docker boot

As another alternative option, you could [install eagle package in sandbox](https://eagle.incubator.apache.org/docs/deployment-in-sandbox.html) manualy as well.

## Building Eagle
Eagle is built using [Apache Maven](https://maven.apache.org/). To build Eagle, run:

    mvn -DskipTests clean package

After successfully building, you will find eagle binary tarball under _eagle-assembly/target/_

## Get Help
The fastest way to get response from eagle community is to send email to the mail list [dev@eagle.incubator.apache.org](mailto:dev@eagle.incubator.apache.org),
and remember to subscribe our mail list via [dev-subscribe@eagle.incubator.apache.org](mailto:dev-subscribe@eagle.incubator.apache.org)

## FAQ
[https://cwiki.apache.org/confluence/display/EAG/FAQ](https://cwiki.apache.org/confluence/display/EAG/FAQ)

## License
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). More details, please refer to [LICENSE](LICENSE) file.
