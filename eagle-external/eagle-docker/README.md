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

# Eagle in Docker

> Docker image for Apache Eagle https://hub.docker.com/r/apacheeagle/sandbox

This is docker container for eagle to help users to have a quick preview about eagle features. 
And this project is to build apache/eagle images and provide eagle-functions to start the containers of eagle.

## Prerequisite
* [Apache Maven](https://maven.apache.org)
* [NPM](https://www.npmjs.com)
* [Docker](https://www.docker.com)
 	* [Mac OS X](http://docs.docker.com/mac/started)
 	* [Linux](http://docs.docker.com/linux/started)
 	* [Windows](http://docs.docker.com/windows/started)

## Getting Started
The fastest way to get started with Eagle is to run with [docker](https://github.com/docker/docker) by one of following options:

* Pull latest eagle docker image from [docker hub](https://hub.docker.com/r/apacheeagle/sandbox/) directly:

        docker pull apacheeagle/sandbox

  Then run eagle docker image

      docker run -p 9099:9099 -p 8080:8080 -p 8744:8744 -p 2181:2181 -p 2888:2888 -p 6667:6667 -p 60020:60020 \
        -p 60030:60030 -p 60010:60010 -d --dns 127.0.0.1 --entrypoint /usr/local/serf/bin/start-serf-agent.sh \
        -e KEYCHAIN= --env EAGLE_SERVER_HOST=sandbox.eagle.incubator.apache.org --name sandbox \
        -h sandbox.eagle.incubator.apache.org --privileged=true apacheeagle/sandbox:latest \
        --tag ambari-server=true
      docker run -it --rm -e EXPECTED_HOST_COUNT=1 -e BLUEPRINT=hdp-singlenode-eagle --link sandbox:ambariserver\
        --entrypoint /bin/sh apacheeagle/sandbox:latest -c /tmp/install-cluster.sh

      
* Build eagle docker image from source code with [eagle-docker](eagle-external/eagle-docker)

        git clone https://github.com/apache/incubator-eagle.git
        cd incubator-eagle && ./eagle-docker boot

## Usage ##
Basic usage of the entry script of eagle-docker: [bin/eagle-docker.sh](bin/eagle-docker.sh)
 
    Usage: ./eagle-docker [options] [command]
    
    Apache Eagle Docker Image : apacheeagle/sandbox:latest
    
    Commands:
      build           Build eagle docker image
      deploy          Deploy eagle docker image
      start           Start eagle docker instance
      stop            Stop eagle docker instance
      status          List eagle docker image and instance status
      clean           Clean docker image and instance
      shell           Execute docker instance bash, default: eagle-sandbox
      boot            Simply bootstrap eagle docker by building then deploying
    
    Options:
      --node [num]    Docker instances node number, default is 1
      --help          Display eagle docker image usage information

## Advanced
1. **Build Image**: Go to the root directory where the [Dockerfile](Dockerfile) is in, build image with following command:
 
        docker built -t apacheeagle/sandbox . 
 
    > The docker image is named `apacheeagle/sandbox`. Eagle docker image is based on [`ambari:1.7.0`](https://github.com/sequenceiq/docker-ambari), it will install ganglia, hbase,hive,storm,kafka and so on in this image. Add startup script and buleprint file into image. 

2. **Verify Image**: After building the `apacheeagle/sandbox` image successfully, verify the images and could find eagle image.

        docker images

3. **Deploy Image**: This project also provides helper functions in script [eagle-lib.sh](bin/eagle-lib.sh) for convenience.
  
        # Firstly, load the helper functions into context
        source eagle-functions
            
        # Secondly, start to deploy eagle cluster
    
        # (1) start single-node container
        eagle-deploy-cluster 1 

        # (2) Or muti-node containers
        eagle-deploy-cluster 3 

4. **Find IP and Port Mapping**: After the container is up and running. The first thing you need to do is finding the IP address and port mapping of the docker container:

        docker inspect -f '{{ .NetworkSettings.IPAddress }}' eagle-server
        docker ps

5. **Start to use Eagle**: Congratulations! You are able to start using Eagle now. Please open eagle ui at following address (username: ADMIN, password: secret by default)

        http://{{container_ip}}:9099/eagle-service  

6. **Manage Eagle Cluster**: This step is about how to managing the eagle cluster though not must-have at starting. Eagle docker depends on Ambari to manage the cluster infrastructure of Eagle. Following are some helpful links:

  * Ambari UI: `http://{{container_ip}}:8080` (username: ADMIN, password: ADMIN)
  * Storm UI: `http://{{container_ip}}:8744`

## Get Help
The fastest way to get response from eagle community is to send email to the mail list [dev@eagle.incubator.apache.org](mailto:dev@eagle.incubator.apache.org),
and remember to subscribe our mail list via [dev-subscribe@eagle.incubator.apache.org](mailto:dev-subscribe@eagle.incubator.apache.org)

## FAQ
[https://cwiki.apache.org/confluence/display/EAG/FAQ#FAQ-EagleDocker](https://cwiki.apache.org/confluence/display/EAG/FAQ#FAQ-EagleDocker)

## License
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). More details, please refer to [LICENSE](LICENSE) file.
