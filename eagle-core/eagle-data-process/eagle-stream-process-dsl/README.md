Eagle Declarative Streaming DSL
===============================

DSL Format
----------

	{
		config {
		  execution.environment.config = someValue
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
* [x] Compile DSL Configure to Pipeline model
* [x] Compile Pipeline model to Stream Execution Graph
* [x] Submit Stream Execution Graph to actual running environment say storm
* [x] Support Alert and Persistence for metric monitoring
* [ ] Extensible stream module management
	* [ ] Automatically scan and register module
* [ ] Pipeline runner CLI tool and shell script
* [ ] Decouple pipeline compiler and scheduler into individual modules
* [ ] Stream Pipeline Scheduler
* [ ] Graph editor to define streaming graph in UI
* [?] JSON/Config & Scala Case Class Mapping (https://github.com/scala/pickling)
* [?] Raw message structure oriented programing is a little ugly, we should define a generic message/event consist of [payload:stream/timestamp/serializer/deserializer,data:message]
* [ ] Provide stream schema inline and send to metadata when submitting
* [ ] UI should support specify executorId when defining new stream
* [ ] Lack of a entity named StreamEntity for the workflow of defining topology&policy end-to-end
* [ ] Fix multi-configure bug
* [ ] Fix configuration conflict

