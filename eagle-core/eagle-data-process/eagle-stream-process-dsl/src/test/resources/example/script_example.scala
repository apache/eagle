#!/bin/bash
exec scala -classpath "/Users/hchen9/Workspace/incubator-eagle/eagle-topology-assembly/target/eagle-topology-0.3.0-assembly.jar" "$0" "$@"
!#
import org.apache.eagle.stream.dsl.universal._

init[storm](args)

// define("metric") as("name" -> 'string, "value" -> 'double, "timestamp" -> 'long) from kafka parallism 10
//define("metric") as("name" -> 'string, "value" -> 'double, "timestamp" -> 'long) from Range(1,10000) parallism 1

define("number") from Range(1,10000) parallism 1

// filter ("metric") groupBy 0 by {line:Map[String,AnyRef] => line}

"number" groupBy 0 to console parallism 1

submit()