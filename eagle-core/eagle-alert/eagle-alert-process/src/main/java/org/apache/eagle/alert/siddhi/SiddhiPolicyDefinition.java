/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.siddhi;

import org.apache.eagle.alert.config.AbstractPolicyDefinition;

/**
 * siddhi policy definition has the following format
 * {
        "type":"SiddhiCEPEngine",
		"expression" : "from every b1=HeapUsage[metric == 'eagle.metric.gc'] -> a1=FullGCEvent[eventName == 'full gc'] -> b2=HeapUsage[metric == b1.metric and host == b1.host and value >= b1.value * 0.8] within 100 sec select a1.eventName, b1.metric, b2.timestamp, 60 as timerange insert into GCMonitor; "
	}
 */
public class SiddhiPolicyDefinition extends AbstractPolicyDefinition {
	private String expression;

	public String getExpression() {
		return expression;
	}
	public void setExpression(String expression) {
		this.expression = expression;
	}
	
	@Override
	public String toString(){
		return expression;
	}
}
