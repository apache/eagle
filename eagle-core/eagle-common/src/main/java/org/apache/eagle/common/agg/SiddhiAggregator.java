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
package org.apache.eagle.common.agg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Since 8/3/16.
 */
public class SiddhiAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(SiddhiAggregator.class);
    private TimeBatchWindowSpec spec;
    private StreamDefinition sd;
    private InputHandler input;

    public SiddhiAggregator(TimeBatchWindowSpec spec, StreamDefinition sd, final AggregateHandler handler) {
        this.spec = spec;
        this.sd = sd;

        Map<String, Integer> colIndices = new HashMap<>();
        List<String> colNames = new ArrayList<>();
        int i = 0;
        for (String col : spec.groupby.cols) {
            colIndices.put(col, i++);
            colNames.add(col);
        }
        for (Agg agg : spec.aggs) {
            colIndices.put(agg.alias, i++);
            colNames.add(agg.alias);
        }

        String query = buildSiddhiAggQuery();
        SiddhiManager sm = new SiddhiManager();
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(query);

        input = runtime.getInputHandler("s");

        runtime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                List<AggregateResult> rows = new ArrayList<AggregateResult>();
                for (Event e : inEvents) {
                    AggregateResult result = new AggregateResult(e.getData(), colIndices, colNames);
                    rows.add(result);
                }
                handler.onAggregate(rows);
            }
        });
        runtime.start();
    }

    public void add(Object[] data) throws Exception {
        input.send(data);
    }

    /**
     * example siddhi query
     * String ql = "define stream s (host string, timestamp long, metric string, site string, value double);" +
     * "@info(name='query') " +
     * " from s[metric == \"missingblocks\"]#window.externalTimeBatch(timestamp, 1 min, 0) select host, count(value) as avg group by host insert into tmp; ";
     *
     * @return
     */
    private String buildSiddhiAggQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append("define stream s(");
        if (sd.columns == null || sd.columns.size() == 0) {
            throw new IllegalStateException("input stream should contains at least one column");
        }
        for (Column col : sd.columns) {
            appendColumnDef(sb, col);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(");");

        sb.append(" @info(name='query') ");
        sb.append("from s[");
        sb.append(spec.filter);
        sb.append("]#window.externalTimeBatch(");
        sb.append(spec.timestampColumn);
        sb.append(",");
        sb.append(spec.windowDuration);
        sb.append(",");
        sb.append(spec.start);
        sb.append(")");
        sb.append(" select ");
        for (String gbField : spec.groupby.cols) {
            sb.append(gbField);
            sb.append(",");
        }
        if (spec.aggs == null) {
            throw new IllegalStateException("at least one aggregate function should be present");
        }
        for (Agg agg : spec.aggs) {
            sb.append(agg.function);
            sb.append("(");
            sb.append(agg.field);
            sb.append(") as ");
            sb.append(agg.alias);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" group by ");
        for (String gbField : spec.groupby.cols) {
            sb.append(gbField);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" insert into tmp;");
        LOG.info("query : " + sb.toString());
        return sb.toString();
    }

    private void appendColumnDef(StringBuilder sb, Column col) {
        sb.append(col.name);
        sb.append(" ");
        sb.append(col.type);
    }
}
