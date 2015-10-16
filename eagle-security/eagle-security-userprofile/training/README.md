User Profiling Offline Training 
===============================

Quick Start
-----------
1. Compile and build package
	
		mvn clean compile package -DskipTests
	
2. Run with following command or directly on IDE
	
		spark-submit --class eagle.security.auditlog.AuditlogTrainingMain --master local target/eagle-security-mltraining-spark-0.1.0.jar

Problems
--------
1. **Problem**: 2015-07-19 17:18:42,083 ERROR [main] spark.SparkContext (Logging.scala:logError(96)) - Error initializing SparkContext. java.net.BindException: Failed to bind to: /10.249.64.134:0: Service 'sparkDriver' failed after 16 retries!
   **Solution**: Add environment variables

		export SPARK_LOCAL_IP=127.0.0.1
		export SPARK_MASTER_IP=127.0.0.1

2. **Problem**: Detected both slf4j-log4j12 and log4j-over-slf4j in classpath

   **Solution**: Exclude log4 and slf4j related dependencies

		<exclusions>
		<exclusion>
		   <groupId>org.slf4j</groupId>
		   <artifactId>slf4j-log4j12</artifactId>
		</exclusion>
		<exclusion>
		   <groupId>org.slf4j</groupId>
		   <artifactId>log4j-over-slf4j</artifactId>
		</exclusion>
		</exclusions>

3. **Problem**: fasterxml related class method not found

   **Solution**: Exclude following modules in version `2.4.1`:
   	
		"com.fasterxml.jackson.core:jackson-core"
		"com.fasterxml.jackson.core:jackson-databind"
		"com.fasterxml.jackson.module:jackson-module-scala_2.10" 
		
   but explicitly use `2.3.1` instead

4. **Problem**: “org.apache.commons.math3.exception.MathIllegalArgumentException: insufficient data: only 10 rows and 1 columns.” breaks spark on distributed environment, but works locally.

   **Solution**: It's because Spark natively depends on math3 version "3.1.1".

5. **Problem**: Exception in thread "delete Spark temp dirs" java.lang.NoClassDefFoundError: Could not initialize class org.apache.log4j.LogManager

   **Solution**: TODO

