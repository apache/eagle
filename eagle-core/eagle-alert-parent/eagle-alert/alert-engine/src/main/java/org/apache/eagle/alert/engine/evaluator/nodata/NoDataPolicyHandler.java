/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.evaluator.nodata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandler;
import org.apache.eagle.alert.engine.evaluator.impl.DistinctValuesInTimeWindow;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.utils.TimePeriodUtils;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Since 6/28/16.
 * No Data Policy engine
 * based on the following information
 * 1. stream definition: group by columns
 * 2. timestamp field: timestamp column
 * 3. wiri safe time window: how long window is good for full set of wiri
 * 4. wisb: full set
 *
 * No data policy definition should include
 * fixed fields and dynamic fields
 * fixed fields are leading fields : windowPeriod, type, numOfFields, f1_name, f2_name
 * dynamic fields depend on wisb type.
 */
public class NoDataPolicyHandler implements PolicyStreamHandler{
    private static final Logger LOG = LoggerFactory.getLogger(NoDataPolicyHandler.class);
    private Map<String, StreamDefinition> sds;

    // wisb(what is should be) set for expected full set value of multiple columns
    @SuppressWarnings("rawtypes")
    private volatile Set wisbValues = null;
    private volatile List<Integer> wisbFieldIndices = new ArrayList<>();
    // reuse PolicyDefinition.defintion.value field to store full set of values separated by comma
    private volatile PolicyDefinition policyDef;
    private volatile DistinctValuesInTimeWindow distinctWindow;
    private volatile Collector<AlertStreamEvent> collector;
    private volatile PolicyHandlerContext context;
    private volatile NoDataWisbType wisbType;

    public NoDataPolicyHandler(Map<String, StreamDefinition> sds){
        this.sds = sds;
    }
    @Override
    public void prepare(Collector<AlertStreamEvent> collector, PolicyHandlerContext context) throws Exception {
        this.collector = collector;
        this.context = context;
        this.policyDef = context.getPolicyDefinition();
        List<String> inputStreams = policyDef.getInputStreams();
        // validate inputStreams has to contain only one stream
        if(inputStreams.size() != 1)
            throw new IllegalArgumentException("policy inputStream size has to be 1 for no data alert");
        // validate outputStream has to contain only one stream
        if(policyDef.getOutputStreams().size() != 1)
            throw new IllegalArgumentException("policy outputStream size has to be 1 for no data alert");

        String is = inputStreams.get(0);
        StreamDefinition sd = sds.get(is);

        String policyValue = policyDef.getDefinition().getValue();
        // assume that no data alert policy value consists of "windowPeriod, type, numOfFields, f1_name, f2_name, f1_value, f2_value, f1_value, f2_value}
        String[] segments = policyValue.split(",");
        long windowPeriod = TimePeriodUtils.getMillisecondsOfPeriod(Period.parse(segments[0]));
        distinctWindow = new DistinctValuesInTimeWindow(windowPeriod);
        this.wisbType = NoDataWisbType.valueOf(segments[1]);
        // for provided wisb values, need to parse, for dynamic wisb values, it is computed through a window
        if(wisbType == NoDataWisbType.provided) {
            wisbValues = new NoDataWisbProvidedParser().parse(segments);
        }
        // populate wisb field names
        int numOfFields = Integer.parseInt(segments[2]);
        for(int i = 3; i < 3+numOfFields; i++){
            String fn = segments[i];
            wisbFieldIndices.add(sd.getColumnIndex(fn));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void send(StreamEvent event) throws Exception {
        Object[] data = event.getData();
        List<Object> columnValues = new ArrayList<>();
        for(int i=0; i<wisbFieldIndices.size(); i++){
            Object o = data[wisbFieldIndices.get(i)];
            // convert value to string
            columnValues.add(o.toString());
        }
        distinctWindow.send(columnValues, event.getTimestamp());
        Set wiriValues = distinctWindow.distinctValues().keySet();

        LOG.debug("window slided: {}, with wiri: {}", distinctWindow.windowSlided(), distinctWindow.distinctValues());

        if(distinctWindow.windowSlided()) {
            compareAndEmit(wisbValues, wiriValues, event);
        }

        if(wisbType == NoDataWisbType.dynamic) {
            // deep copy
            wisbValues = new HashSet<>(wiriValues);
        }
    }

    @SuppressWarnings("rawtypes")
    private void compareAndEmit(Set wisb, Set wiri, StreamEvent event){
        // compare with wisbValues if wisbValues are already there for dynamic type
        Collection noDataValues = CollectionUtils.subtract(wisb, wiri);
        LOG.debug("nodatavalues:" + noDataValues + ", wisb: " + wisb + ", wiri: " + wiri);
        if (noDataValues != null && noDataValues.size() > 0) {
            LOG.info("No data alert is triggered with no data values {} and wisb {}", noDataValues, wisbValues);
            AlertStreamEvent alertEvent = createAlertEvent(event.getTimestamp(), event.getData());
            collector.emit(alertEvent);
        }
    }

    private AlertStreamEvent createAlertEvent(long timestamp, Object[] triggerEvent){
        String is = policyDef.getInputStreams().get(0);
        StreamDefinition sd = sds.get(is);

        AlertStreamEvent event = new AlertStreamEvent();
        event.setTimestamp(timestamp);
        event.setData(triggerEvent);
        event.setStreamId(policyDef.getOutputStreams().get(0));
        event.setPolicy(context.getPolicyDefinition());
        if (this.context.getParentEvaluator() != null) {
            event.setCreatedBy(context.getParentEvaluator().getName());
        }
        event.setCreatedTime(System.currentTimeMillis());
        event.setSchema(sd);
        return event;
    }

    @Override
    public void close() throws Exception {

    }
}
