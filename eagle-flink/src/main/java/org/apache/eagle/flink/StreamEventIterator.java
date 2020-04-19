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

package org.apache.eagle.flink;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
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

	private static List<StreamEvent> data = Arrays.asList(
		new StreamEvent("testStream_1", 0L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 10L, new Object[]{100}),
		new StreamEvent("testStream_1", 10L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 20L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 20L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 20L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 30L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 40L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 50L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 70L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 90L, new Object[]{100}),
		new StreamEvent("testStream_1", 100L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 200L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 210L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 220L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 230L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 250L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 260L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 270L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 300L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 400L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 600L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 1000L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 12000L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 12001L, new Object[]{188.23}),
		new StreamEvent("testStream_1", 12002L, new Object[]{188.23})
	);
}
