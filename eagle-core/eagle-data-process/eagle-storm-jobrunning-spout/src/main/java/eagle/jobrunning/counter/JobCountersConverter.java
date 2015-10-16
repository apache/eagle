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
package eagle.jobrunning.counter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eagle.jobrunning.yarn.model.Counter;
import eagle.jobrunning.yarn.model.CounterGroup;
import eagle.jobrunning.yarn.model.JobCounters;

public class JobCountersConverter {

	public static eagle.jobrunning.counter.JobCounters convert(JobCounters jobCounters) {
		final eagle.jobrunning.counter.JobCounters result = new eagle.jobrunning.counter.JobCounters();
		final List<CounterGroup> groups = jobCounters.getCounterGroup();
		if (groups != null) {
			for (CounterGroup group : groups) {
				final String groupName = group.getCounterGroupName();
				final Map<String, Long> counterMap = new HashMap<String, Long>();
				result.getCounters().put(groupName, counterMap);
				final List<Counter> counters = group.getCounter();
				for (Counter counter : counters) {
					final String counterName = counter.getName();
					final Long totalValue =  counter.getTotalCounterValue();
					counterMap.put(counterName, totalValue);
				}
			}
		}
		return result;
	}
}
