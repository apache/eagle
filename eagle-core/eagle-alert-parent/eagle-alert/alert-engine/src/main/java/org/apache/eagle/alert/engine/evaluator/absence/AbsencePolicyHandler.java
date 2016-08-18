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
package org.apache.eagle.alert.engine.evaluator.absence;

import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandler;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.utils.AlertStreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Since 7/6/16.
 *  * policy would be like:
 * {
 "name": "absenceAlertPolicy",
 "description": "absenceAlertPolicy",
 "inputStreams": [
 "absenceAlertStream"
 ],
 "outputStreams": [
 "absenceAlertStream_out"
 ],
 "definition": {
 "type": "absencealert",
 "value": "1,jobID,job1,daily_rule,14:00:00,15:00:00"
 },
 "partitionSpec": [
 {
 "streamId": "absenceAlertStream",
 "type": "GROUPBY",
 "columns" : ["jobID"]
 }
 ],
 "parallelismHint": 2
 }
 */
public class AbsencePolicyHandler implements PolicyStreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AbsencePolicyHandler.class);
    private Map<String, StreamDefinition> sds;
    private volatile PolicyDefinition policyDef;
    private volatile Collector<AlertStreamEvent> collector;
    private volatile PolicyHandlerContext context;
    private volatile List<Integer> expectFieldIndices = new ArrayList<>();
    private volatile List<Object> expectValues = new ArrayList<>();
    private AbsenceAlertDriver driver;

    public AbsencePolicyHandler(Map<String, StreamDefinition> sds){
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
            throw new IllegalArgumentException("policy inputStream size has to be 1 for absence alert");
        // validate outputStream has to contain only one stream
        if(policyDef.getOutputStreams().size() != 1)
            throw new IllegalArgumentException("policy outputStream size has to be 1 for absence alert");

        String is = inputStreams.get(0);
        StreamDefinition sd = sds.get(is);

        String policyValue = policyDef.getDefinition().getValue();

        // Assume that absence alert policy value consists of
        // "numOfFields, f1_name, f2_name, f1_value, f2_value, absence_window_rule_type, startTimeOffset, endTimeOffset"
        String[] segments = policyValue.split(",\\s*");
        int offset = 0;
        // populate wisb field names
        int numOfFields = Integer.parseInt(segments[offset++]);
        for(int i = offset; i < offset+numOfFields; i++){
            String fn = segments[i];
            expectFieldIndices.add(sd.getColumnIndex(fn));
        }
        offset += numOfFields;
        for(int i = offset; i < offset+numOfFields; i++){
            String fn = segments[i];
            expectValues.add(fn);
        }
        offset += numOfFields;
        String absence_window_rule_type = segments[offset++];
        AbsenceDailyRule rule = new AbsenceDailyRule();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date t1 = sdf.parse(segments[offset++]);
        rule.startOffset = t1.getTime();
        Date t2 = sdf.parse(segments[offset++]);
        rule.endOffset = t2.getTime();
        AbsenceWindowGenerator generator = new AbsenceWindowGenerator(rule);
        driver = new AbsenceAlertDriver(expectValues, generator);
    }

    @Override
    public void send(StreamEvent event) throws Exception {
        Object[] data = event.getData();
        List<Object> columnValues = new ArrayList<>();
        for(int i=0; i<expectFieldIndices.size(); i++){
            Object o = data[expectFieldIndices.get(i)];
            // convert value to string
            columnValues.add(o.toString());
        }

        boolean isAbsenceAlert = driver.process(columnValues, event.getTimestamp());

        // Publishing alerts.
        if (isAbsenceAlert) {
            AlertStreamEvent alertEvent = AlertStreamUtils.createAlertEvent(event, context, sds);
            collector.emit(alertEvent);
        }
    }

    @Override
    public void close() throws Exception {

    }
}