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

# Apache Eagle

>  Secure Hadoop Data in Real Time

Apache Eagle is an open source monitoring solution to instantly identify access to sensitive data, recognize attacks, malicious activities in Hadoop and take actions in real time.

For more details, please visit [https://eagle.incubator.apache.org](https://eagle.incubator.apache.org)

## Documentation
You can find the latest Eagle documentation on the [https://eagle.incubator.apache.org](https://eagle.incubator.apache.org/docs). This [README](README) file only contains basic setup instructions.

## Getting Started
The fastest way to get started with Eagle is using [docker](https://github.com/docker/docker) or [sandbox](http://hortonworks.com/products/hortonworks-sandbox/)

* Eagle in Docker: firstly install docker engine and then execute:
    
    ./eagle-docker boot

* Eagle in Sandbox: see [https://eagle.incubator.apache.org/docs/deployment-in-sandbox.html](https://eagle.incubator.apache.org/docs/deployment-in-sandbox.html)

## Building Eagle
Eagle is built using [Apache Maven](https://maven.apache.org/). To build Eagle, run:

    mvn -DskipTests clean package

After successfully building, you will find eagle binary tarball under _eagle-assembly/target/_

## Get Help
The fastest way to get response from eagle community is to send email to the mail list [dev@eagle.incubator.apache.org](mailto:dev@eagle.incubator.apache.org),
and remember to subscribe our mail list via [dev-subscribe@eagle.incubator.apache.org](mailto:dev-subscribe@eagle.incubator.apache.org)

## License
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). More details, please refer to [LICENSE](LICENSE) file.
