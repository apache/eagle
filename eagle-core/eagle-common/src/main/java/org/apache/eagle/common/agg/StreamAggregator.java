/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.common.agg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Since 8/4/16.
 */
public class StreamAggregator {
    public static StreamAggregatorBuilder builder() {
        return new StreamAggregatorBuilder();
    }

    public static class StreamAggregatorBuilder {
        private Groupby gb = new Groupby();
        private String windowDuration;
        private long start;
        private String filter;
        private List<Agg> aggs = new ArrayList<>();
        private String timestampColumn;
        private StreamDefinition sd = new StreamDefinition();
        private AggregateHandler handler;

        public StreamAggregatorBuilder groupby(String... gbFields) {
            gb.cols = Arrays.asList(gbFields);
            return this;
        }

        public StreamAggregatorBuilder window(String windowDuration) {
            window(windowDuration, 0);
            return this;
        }

        public StreamAggregatorBuilder window(String windowDuration, long start) {
            this.windowDuration = windowDuration;
            this.start = start;
            return this;
        }

        public StreamAggregatorBuilder timeColumn(String timestampColumn) {
            this.timestampColumn = timestampColumn;
            return this;
        }

        public StreamAggregatorBuilder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public StreamAggregatorBuilder agg(String function, String field, String alias) {
            Agg agg = new Agg();
            agg.function = function;
            agg.field = field;
            agg.alias = alias;
            aggs.add(agg);
            return this;
        }

        public StreamAggregatorBuilder columnDef(String colName, String colType) {
            Column col = new Column();
            col.name = colName;
            col.type = colType;
            if (sd.columns == null) {
                sd.columns = new ArrayList<>();
            }
            sd.columns.add(col);
            return this;
        }

        public StreamAggregatorBuilder streamDef(StreamDefinition sd) {
            this.sd = sd;
            return this;
        }

        public StreamAggregatorBuilder aggregateHandler(AggregateHandler handler) {
            this.handler = handler;
            return this;
        }

        public SiddhiAggregator build() {
            TimeBatchWindowSpec spec = new TimeBatchWindowSpec();
            spec.aggs = aggs;
            spec.filter = filter;
            spec.groupby = gb;
            spec.start = start;
            spec.timestampColumn = timestampColumn;
            spec.windowDuration = windowDuration;
            SiddhiAggregator aggregator = new SiddhiAggregator(spec, sd, handler);
            return aggregator;
        }
    }
}
