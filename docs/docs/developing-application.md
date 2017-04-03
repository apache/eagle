# Introduction

[Applications](applications) in Eagle include process component and view component. Process component normally refers to storm topology or spark stream job which processes incoming data, while viewing component normally refers to GUI hosted in Eagle UI. 

[Application Framework](getting-started/#eagle-framework) targets at solving the problem of managing application lifecycle and presenting uniform views to end users.
 
Eagle application framework is designed for end-to-end lifecycle of applications including:

* **Development**: application development and framework development

* **Testing**.

* **Installation**: package management with SPI/Providers.xml

* **Management**: manage applications through REST API

---

# Quick Start

* Fork and clone eagle source code repository using GIT.

        git clone https://github.com/apache/incubator-eagle.git

* Run Eagle Server : execute “org.apache.eagle.server.ServerDebug” under eagle-server in IDE or with maven command line.

        org.apache.eagle.server.ServerDebug

* Access current available applications through API.

        curl -XGET  http://localhost:9090/rest/apps/providers

* Create Site through API.

        curl -H "Content-Type: application/json" -X POST  http://localhost:9090/rest/sites --data '{
             "siteId":"test_site",
             "siteName":"Test Site",
             "description":"This is a sample site for test",
             "context":{
                  "type":"FAKE_CLUSTER",
                  "url":"http://localhost:9090",
                  "version":"2.6.4",
                  "additional_attr":"Some information about the face cluster site"
             }
        }'

* Install Application through API.

        curl -H "Content-Type: application/json" -X POST http://localhost:9090/rest/apps/install --data '{
             "siteId":"test_site",
             "appType":"EXAMPLE_APPLICATION",
             "mode":"LOCAL"
        }'

* Start Application  (uuid means installed application uuid).

        curl -H "Content-Type: application/json" –X POST http://localhost:9090/rest/apps/start --data '{
             "uuid":"9acf6792-60e8-46ea-93a6-160fb6ef0b3f"
        }'

* Stop Application (uuid means installed application uuid).

        curl -XPOST http://localhost:9090/rest/apps/stop '{
         "uuid": "9acf6792-60e8-46ea-93a6-160fb6ef0b3f"
        }'

* Uninstall Application (uuid means installed application uuid).

        curl -XDELETE http://localhost:9090/rest/apps/uninstall '{
         "uuid": "9acf6792-60e8-46ea-93a6-160fb6ef0b3f"
        }'

---

# Create Application

Each application should be developed under independent modules (including backend code and front-end code).

Here is a typical code structure of a new application as following:

```
eagle-app-example/
├── pom.xml
├── src
│   ├── main
│   │   ├── java
│   │   │   └── org
│   │   │       └── apache
│   │   │           └── eagle
│   │   │               └── app
│   │   │                   └── example
│   │   │                       ├── ExampleApplicationProvider.java
│   │   │                       ├── ExampleStormApplication.java
│   │   ├── resources
│   │   │   └── META-INF
│   │   │       ├── providers
│   │   │       │   └── org.apache.eagle.app.example.ExampleApplicationProvider.xml
│   │   │       └── services
│   │   │           └── org.apache.eagle.app.spi.ApplicationProvider
│   │   └── webapp
│   │       ├── app
│   │       │   └── apps
│   │       │       └── example
│   │       │           └── index.html
│   │       └── package.json
│   └── test
│       ├── java
│       │   └── org
│       │       └── apache
│       │           └── eagle
│       │               └── app
│       │                   ├── example
│       │                   │   ├── ExampleApplicationProviderTest.java
│       │                   │   └── ExampleApplicationTest.java
│       └── resources
│           └── application.conf
```

**Eagle Example Application** - [eagle-app-example](https://github.com/haoch/incubator-eagle/tree/master/eagle-examples/eagle-app-example)

**Description** - A typical eagle application is mainly consisted of:

* **Application**: define core execution process logic inheriting from org.apache.eagle.app.Application, which is also implemented ApplicationTool to support Application to run as standalone process like a Storm topology  through command line.

* **ApplicationProvider**: the interface to package application with descriptor metadata, also used as application SPI to dynamically load new application types.

* **META-INF/providers/${APP_PROVIDER_CLASS_NAME}.xml**: support to easily describe application’s descriptor with declarative XML like:

        <application>
           <type>EXAMPLE_APPLICATION</type>
           <name>Example Monitoring Application</name>
           <version>0.5.0-incubating</version>
           <configuration>
               <property>
                   <name>message</name>
                   <displayName>Message</displayName>
                   <value>Hello, example application!</value>
                   <description>Just an sample configuration property</description>
               </property>
           </configuration>
           <streams>
               <stream>
                   <streamId>SAMPLE_STREAM_1</streamId>
                   <description>Sample output stream #1</description>
                   <validate>true</validate>
                   <timeseries>true</timeseries>
                   <columns>
                       <column>
                           <name>metric</name>
                           <type>string</type>
                       </column>
                       <column>
                           <name>source</name>
                           <type>string</type>
                       </column>
                       <column>
                           <name>value</name>
                           <type>double</type>
                           <defaultValue>0.0</defaultValue>
                       </column>
                   </columns>
               </stream>
               <stream>
                   <streamId>SAMPLE_STREAM_2</streamId>
                   <description>Sample output stream #2</description>
                   <validate>true</validate>
                   <timeseries>true</timeseries>
                   <columns>
                       <column>
                           <name>metric</name>
                           <type>string</type>
                       </column>
                       <column>
                           <name>source</name>
                           <type>string</type>
                       </column>
                       <column>
                           <name>value</name>
                           <type>double</type>
                           <defaultValue>0.0</defaultValue>
                       </column>
                   </columns>
               </stream>
           </streams>
        </application>

* **META-INF/services/org.apache.eagle.app.spi.ApplicationProvider**: support to dynamically scan and load extensible application provider using java service provider.

* **webapp/app/apps/${APP_TYPE}**: if the application has web portal, then it could add more web code under this directory and make sure building as following in pom.xml

        <build>
           <resources>
               <resource>
                   <directory>src/main/webapp/app</directory>
                   <targetPath>assets/</targetPath>
               </resource>
               <resource>
                   <directory>src/main/resources</directory>
               </resource>
           </resources>
           <testResources>
               <testResource>
                   <directory>src/test/resources</directory>
               </testResource>
           </testResources>
        </build>

---

# Test Application

* Extend **org.apache.eagle.app.test.ApplicationTestBase** and initialize injector context.

* Access shared service with **@Inject**.

* Test application lifecycle with related web resource.

        @Inject private SiteResource siteResource;
        @Inject private ApplicationResource applicationResource;

        // Create local site
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("test_site");
        siteEntity.setSiteName("Test Site");
        siteEntity.setDescription("Test Site for ExampleApplicationProviderTest");
        siteResource.createSite(siteEntity);
        Assert.assertNotNull(siteEntity.getUuid());

        ApplicationOperations.InstallOperation installOperation = new ApplicationOperations.InstallOperation(
        	"test_site", 
        	"EXAMPLE_APPLICATION", 
        	ApplicationEntity.Mode.LOCAL);
        installOperation.setConfiguration(getConf());
        // Install application
        ApplicationEntity applicationEntity = applicationResource
            .installApplication(installOperation)
            .getData();
        // Start application
        applicationResource.startApplication(new ApplicationOperations.StartOperation(applicationEntity.getUuid()));
        // Stop application
        applicationResource.stopApplication(new ApplicationOperations.StopOperation(applicationEntity.getUuid()));
        // Uninstall application
        applicationResource.uninstallApplication(
        	new ApplicationOperations.UninstallOperation(applicationEntity.getUuid()));
        try {
           applicationResource.getApplicationEntityByUUID(applicationEntity.getUuid());
           Assert.fail("Application instance (UUID: " + applicationEntity.getUuid() + ") should have been uninstalled");
        } catch (Exception ex) {
           // Expected exception
        }

---

# Management & REST API

## ApplicationProviderSPILoader

Default behavior - automatically loading from class path using SPI:

* By default, eagle will load application providers from current class loader.

* If application.provider.dir defined, it will load from external jars’ class loader.

## Application REST API

* API Table

    | Type       | Uri + Class |
    | :--------: | :---------- |
    | **DELETE** | /rest/sites (org.apache.eagle.metadata.resource.SiteResource) |
    | **DELETE** | /rest/sites/{siteId} (org.apache.eagle.metadata.resource.SiteResource) |
    | **GET**    | /rest/sites (org.apache.eagle.metadata.resource.SiteResource) |
    | **GET**    | /rest/sites/{siteId} (org.apache.eagle.metadata.resource.SiteResource) |
    | **POST**   | /rest/sites (org.apache.eagle.metadata.resource.SiteResource) |
    | **PUT**    | /rest/sites (org.apache.eagle.metadata.resource.SiteResource) |
    | **PUT**    | /rest/sites/{siteId} (org.apache.eagle.metadata.resource.SiteResource) |
    | **DELETE** | /rest/apps/uninstall (org.apache.eagle.app.resource.ApplicationResource) |
    | **GET**    | /rest/apps (org.apache.eagle.app.resource.ApplicationResource) |
    | **GET**    | /rest/apps/providers (org.apache.eagle.app.resource.ApplicationResource) |
    | **GET**    | /rest/apps/providers/{type} (org.apache.eagle.app.resource.ApplicationResource) |
    | **GET**    | /rest/apps/{appUuid} (org.apache.eagle.app.resource.ApplicationResource) |
    | **POST**   | /rest/apps/install (org.apache.eagle.app.resource.ApplicationResource) |
    | **POST**   | /rest/apps/start (org.apache.eagle.app.resource.ApplicationResource) |
    | **POST**   | /rest/apps/stop (org.apache.eagle.app.resource.ApplicationResource) |
    | **PUT**    | /rest/apps/providers/reload (org.apache.eagle.app.resource.ApplicationResource) |
