# Eagle

>  Secure Hadoop Data in Real Time

Eagle is an Open Source Monitoring solution for Hadoop to instantly identify access to sensitive data, recognize attacks, malicious activities in hadoop and take actions. 

For more details, see the website [http://goeagle.io](http://goeagle.io).

## Use Cases
* Anomalous access detection
* Monitor data access traffic 
* Discover intrusions and security breach
* Discover and prevent sensitive data loss and leaks

## Getting Started
Please refer to [http://goeagle.io/docs/deployment-in-sandbox.html](http://goeagle.io/docs/deployment-in-sandbox.html)

## Building Eagle
Eagle is built using [Apache Maven](https://maven.apache.org/). To build Eagle, run:

    mvn -DskipTests clean package

After successfully building, you will find eagle binary tarball under _[eagle-assembly](eagle-assembly/)/target/_

## Documentation
You can find the latest Eagle documentation on the [project documentation site](http://goeagle.io/docs) or [project wiki](https://github.com/eBay/Eagle/wiki). This [README](README) file only contains basic setup instructions.

## Get Help
We are in the process of applying for apache incubation. Until then if you have any questions please reach out to Arun Manoharan [armanoharan at ebay dot com](mailto:armanoharan@ebay.com)

## License
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). More details, please refer to [LICENSE](LICENSE) file.
