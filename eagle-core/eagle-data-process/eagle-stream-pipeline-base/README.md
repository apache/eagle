Eagle Declarative Streaming DSL
===============================

DSL Format
----------

	{
		config {
		  config.key = configValue
		}

		schema {
		  metricStreamSchema {
		    metric: string
		    value: double
		    timestamp: long
		  }
		}

		dataflow {
		  kafkaSource.source1 {
		    schema = "metricStreamSchema"
		  }
		  kafkaSource.source2 {
		    schema = {
		      metric: string
		      value: double
		      timestamp: long
		    }
		  }
		}
	}

Usage
-----

	val pipeline = Pipeline.parseResource("pipeline.conf")
	val stream = Pipeline.compile(pipeline)
	stream.submit[storm]

Features
--------
* [done] Compile DSL Configure to Pipeline model
* [done] Compile Pipeline model to Stream Execution Graph
* [done] Submit Stream Execution Graph to actual running environment say storm
* [done] Support Alert and Persistence for metric monitoring
* [ ] Extensible stream module management and automatically scan and register module
* [ ] Pipeline runner CLI tool and shell script
* [ ] Decouple pipeline compiler and scheduler into individual modules
* [ ] Stream Pipeline Scheduler
* [ ] Graph editor to define streaming graph in UI
* [?] JSON/Config & Scala Case Class Mapping (https://github.com/scala/pickling)
* [?] Raw message structure oriented programing is a little ugly, we should define a generic message/event consist of [payload:stream/timestamp/serializer/deserializer,data:message]
* [ ] Provide stream schema inline and send to metadata when submitting
* [ ] UI should support specify executorId when defining new stream
* [ ] Lack of a entity named StreamEntity for the workflow of defining topology&policy end-to-end
* [!] Fix configuration conflict, should pass through Config instead of ConfigFactory.load() manually
* [ ] Override application configuration with pipeline configuration
* [ ] Refactor schema registration structure and automatically submit stream schema when submitting pipeline
* [ ] Submit alertStream, alertExecutorId mapping to AlertExecutorService when submitting pipeline
* [x] Supports `inputs` field to define connector