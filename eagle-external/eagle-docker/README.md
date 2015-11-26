# Eagle in Docker

> Docker image for Apache Eagle http://eagle.incubator.apache.org

This is docker container for eagle to help users to have a quick preview about eagle features. 
And this project is to build apache/eagle images and provide eagle-functions to start the containers of eagle.

## Prerequisite
* Docker environment (see [https://www.docker.com](https://www.docker.com/)) 

## Installation & Usage
1. **Build Image**: Go to the root directory where the [Dockerfile](Dockerfile) is in, build image with following command:
 
        docker built -t apache/eagle . 
 
    > The docker image is named `apache/eagle`. Eagle docker image is based on [`ambari:1.7.0`](https://github.com/sequenceiq/docker-ambari), it will install ganglia, hbase,hive,storm,kafka and so on in this image. Add startup script and buleprint file into image. 

2. **Verify Image**: After building the `apache/eagle` image successfully, verify the images and could find eagle image.

        docker images

3. **Deploy Image**: This project also provides helper functions in script [eagle-functions](eagle-functions) for convenience.
  
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

## License
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). More details, please refer to [LICENSE](LICENSE) file.
