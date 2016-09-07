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
package org.apache.eagle.alert.engine.evaluator.nodata;

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandler;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.utils.TimePeriodUtils;
import org.apache.storm.guava.base.Joiner;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NoDataPolicyTimeBatchHandler implements PolicyStreamHandler {
	
	private static final Logger LOG = LoggerFactory.getLogger(NoDataPolicyTimeBatchHandler.class);
	private Map<String, StreamDefinition> sds;
	
	private volatile List<Integer> wisbFieldIndices = new ArrayList<>();
	// reuse PolicyDefinition.defintion.value field to store full set of values
	// separated by comma
	private volatile PolicyDefinition policyDef;
	private volatile Collector<AlertStreamEvent> collector;
	private volatile PolicyHandlerContext context;
	private volatile NoDataWisbType wisbType;
	private volatile DistinctValuesInTimeBatchWindow distinctWindow;

	public NoDataPolicyTimeBatchHandler(Map<String, StreamDefinition> sds){
        this.sds = sds;
    }

	@Override
	public void prepare(Collector<AlertStreamEvent> collector, PolicyHandlerContext context) throws Exception {
		this.collector = collector;
		this.context = context;
		this.policyDef = context.getPolicyDefinition();
		List<String> inputStreams = policyDef.getInputStreams();
		// validate inputStreams has to contain only one stream
		if (inputStreams.size() != 1)
			throw new IllegalArgumentException("policy inputStream size has to be 1 for no data alert");
		// validate outputStream has to contain only one stream
		if (policyDef.getOutputStreams().size() != 1)
			throw new IllegalArgumentException("policy outputStream size has to be 1 for no data alert");

		String is = inputStreams.get(0);
		StreamDefinition sd = sds.get(is);

		String policyValue = policyDef.getDefinition().getValue();
		// assume that no data alert policy value consists of "windowPeriod,
		// type, numOfFields, f1_name, f2_name, f1_value, f2_value, f1_value,
		// f2_value}
		String[] segments = policyValue.split(",");
		this.wisbType = NoDataWisbType.valueOf(segments[1]);
		// for provided wisb values, need to parse, for dynamic wisb values, it
		// is computed through a window
		@SuppressWarnings("rawtypes")
		Set wisbValues = null;
		if (wisbType == NoDataWisbType.provided) {
			wisbValues = new NoDataWisbProvidedParser().parse(segments);
		}
		long windowPeriod = TimePeriodUtils.getMillisecondsOfPeriod(Period.parse(segments[0]));
		distinctWindow = new DistinctValuesInTimeBatchWindow(this, windowPeriod, wisbValues);
		// populate wisb field names
		int numOfFields = Integer.parseInt(segments[2]);
		for (int i = 3; i < 3 + numOfFields; i++) {
			String fn = segments[i];
			wisbFieldIndices.add(sd.getColumnIndex(fn));
		}
	}

	@Override
	public void send(StreamEvent event) throws Exception {
		Object[] data = event.getData();
		
		List<Object> columnValues = new ArrayList<>();
		for (int i = 0; i < wisbFieldIndices.size(); i++) {
			Object o = data[wisbFieldIndices.get(i)];
			// convert value to string
			columnValues.add(o.toString());
		}
		// use local timestamp rather than event timestamp
		distinctWindow.send(event, columnValues, System.currentTimeMillis());
		LOG.debug("event sent to window with wiri: {}", distinctWindow.distinctValues());
	}
	
	@SuppressWarnings("rawtypes")
	public void compareAndEmit(Set wisb, Set wiri, StreamEvent event) {
		// compare with wisbValues if wisbValues are already there for dynamic
		// type
		Collection noDataValues = CollectionUtils.subtract(wisb, wiri);
		LOG.debug("nodatavalues:" + noDataValues + ", wisb: " + wisb + ", wiri: " + wiri);
		if (noDataValues != null && noDataValues.size() > 0) {
			LOG.info("No data alert is triggered with no data values {} and wisb {}", noDataValues, wisb);
			
			String is = policyDef.getOutputStreams().get(0);
			StreamDefinition sd = sds.get(is);
			int timestampIndex = sd.getColumnIndex("timestamp");
			int hostIndex = sd.getColumnIndex("host");
			int originalStreamNameIndex = sd.getColumnIndex("originalStreamName");
			
			for (Object one : noDataValues) {
				Object[] triggerEvent = new Object[sd.getColumns().size()];
				for (int i = 0; i < sd.getColumns().size(); i ++) {
					if (i == timestampIndex) {
						triggerEvent[i] = System.currentTimeMillis();
					} else if (i == hostIndex) {
						triggerEvent[hostIndex] = ((List) one).get(0);
					} else if (i == originalStreamNameIndex) {
						triggerEvent[originalStreamNameIndex] = event.getStreamId();
					} else if (sd.getColumns().size() < i) {
						LOG.error("strema event data have different lenght compare to column definition!");
					} else {
						triggerEvent[i] = sd.getColumns().get(i).getDefaultValue();
					}
				}
				AlertStreamEvent alertEvent = createAlertEvent(sd, event.getTimestamp(), triggerEvent);
				LOG.info(String.format("Nodata alert %s generated and will be emitted", Joiner.on(",").join(triggerEvent)));
				collector.emit(alertEvent);
			}
			
		}
	}

	private AlertStreamEvent createAlertEvent(StreamDefinition sd, long timestamp, Object[] triggerEvent) {
		AlertStreamEvent event = new AlertStreamEvent();
		event.setTimestamp(timestamp);
		event.setData(triggerEvent);
		event.setStreamId(policyDef.getOutputStreams().get(0));
		event.setPolicyId(context.getPolicyDefinition().getName());
		if (this.context.getPolicyEvaluator() != null) {
			event.setCreatedBy(context.getPolicyEvaluator().getName());
		}
		event.setCreatedTime(System.currentTimeMillis());
		event.setSchema(sd);
		return event;
	}

	@Override
	public void close() throws Exception {

	}

}
