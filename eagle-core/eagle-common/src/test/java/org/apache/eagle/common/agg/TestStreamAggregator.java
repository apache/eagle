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

import org.junit.Test;

import java.util.List;

/**
 * Since 8/4/16.
 */
public class TestStreamAggregator {
    @Test
    public void test() throws Exception{
        SiddhiAggregator aggregator = StreamAggregator.builder()
                .columnDef("host", "string")
                .columnDef("timestamp", "long")
                .columnDef("metric", "string")
                .columnDef("site", "string")
                .columnDef("value", "double")
                .filter("metric==\"missingblocks\"")
                .groupby("host")
                .agg("avg", "value", "avg")
                .timeColumn("timestamp")
                .window("1 min", 0)
                .aggregateHandler(new AggregateHandler() {
                    @Override
                    public void onAggregate(List<AggregateResult> result) {
                        System.out.println(result);
                    }
                })
                .build();

        aggregator.add(new Object[]{"host1", 1000L, "missingblocks", "site1", 10.0});
        aggregator.add(new Object[]{"host2", 2000L, "missingblocks", "site1", 16.0});
        aggregator.add(new Object[]{"host3", 2000L, "missingblocks", "site1", 11.0});
        aggregator.add(new Object[]{"host1", 21000L, "missingblocks", "site1", 20.0});

        aggregator.add(new Object[]{"host1", 61000L, "missingblocks", "site1", 14.0});
        aggregator.add(new Object[]{"host2", 61500L, "missingblocks", "site1", 14.0});
        aggregator.add(new Object[]{"host3", 62000L, "missingblocks", "site1", 13.0});
        aggregator.add(new Object[]{"host2", 63500L, "missingblocks", "site1", 19.0});

        aggregator.add(new Object[]{"host1", 121000L, "missingblocks", "site1", 14.0});
        aggregator.add(new Object[]{"host2", 121000L, "missingblocks", "site1", 14.0});
        aggregator.add(new Object[]{"host3", 122000L, "missingblocks", "site1", 13.0});
    }
}
