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
* [x] Compile DSL Configure to Pipeline model
* [x] Compile Pipeline model to Stream Execution Graph
* [x] Submit Stream Execution Graph to actual running environment say storm
* [x] Support Alert and Persistence for metric monitoring
* [ ] Extensible stream module management and automatically scan and register module
* [x] Pipeline runner CLI tool and shell script
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