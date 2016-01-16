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
------

	val pipeline = Pipeline.parseResource("pipeline.conf")
	val stream = Pipeline.compile(pipeline)
	stream.submit[storm]

Features
--------
* Compile DSL Configure to Pipeline model
* Compile Pipeline model to Stream Execution Graph
* Submit Stream Execution Graph to actual running environment say storm

TODO
----
* [ ] Support Alert and Persistence for metric monitoring
* [ ] Extensible stream module management
	* [ ] Automatically scan and register module
* [ ] Pipeline runner CLI tool and shell script
* [ ] Decouple pipeline compiler and scheduler into individual modules
* [ ] Stream Pipeline Scheduler
* [ ] Graph editor to define streaming graph in UI