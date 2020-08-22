/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.flink.test;

import org.apache.eagle.flink.StreamEvent;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * An iterator of StreamEvent events.
 */
final class StreamEventIterator implements Iterator<StreamEvent>, Serializable {

	private static final long serialVersionUID = 1L;

	private static final Timestamp INITIAL_TIMESTAMP = Timestamp.valueOf("2019-01-01 00:00:00");

	private static final long SIX_MINUTES = 6 * 60 * 1000;

	private final boolean bounded;

	private int index = 0;

	private long timestamp;

	static StreamEventIterator bounded() {
		return new StreamEventIterator(true);
	}

	static StreamEventIterator unbounded() {
		return new StreamEventIterator(false);
	}

	private StreamEventIterator(boolean bounded) {
		this.bounded = bounded;
		this.timestamp = INITIAL_TIMESTAMP.getTime();
	}

	@Override
	public boolean hasNext() {
		if (index < data.size()) {
			return true;
		} else if (!bounded) {
			index = 0;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public StreamEvent next() {
		StreamEvent StreamEvent = data.get(index++);
		StreamEvent.setTimestamp(timestamp);
		timestamp += SIX_MINUTES;
		return StreamEvent;
	}

	private static StreamEvent createSampleEvent(long timestamp, String name, double value) {
		return StreamEvent.builder()
				.schema(MockSampleMetadataFactory.createInStreamDef("sampleStream_1"))
				.streamId("sampleStream_1")
				.timestamep(start + timestamp)
				.attributes(new HashMap<String, Object>() {{
					put("name", name);
					put("value", value);
				}}).build();
	}

	private static long start = System.currentTimeMillis();
	private static List<StreamEvent> data = Arrays.asList(
			createSampleEvent(0, "cpu", 60.0),
			createSampleEvent(1, "nic", 10.0),
			createSampleEvent(1, "nic", 20.0),
			createSampleEvent(10, "cpu", 60.0),
			createSampleEvent(10, "cpu", 60.0),
			createSampleEvent(10, "cpu", 60.0),
			createSampleEvent(20, "cpu", 60.0),
			createSampleEvent(30, "cpu", 60.0),
			createSampleEvent(50, "cpu", 60.0),
			createSampleEvent(100, "cpu", 60.0),
			createSampleEvent(120, "cpu", 60.0),
			createSampleEvent(120, "cpu", 60.0),
			createSampleEvent(130, "cpu", 60.0),
			createSampleEvent(160, "cpu", 60.0),
			createSampleEvent(2000, "door", 100.0),
			createSampleEvent(2000, "cpu", 60.0),
			createSampleEvent(2200, "cpu", 60.0),
			createSampleEvent(2500, "cpu", 60.0),
			createSampleEvent(2500, "cpu", 60.0),
			createSampleEvent(2500, "cpu", 60.0),
			createSampleEvent(3000, "cpu", 60.0)
	);
}
