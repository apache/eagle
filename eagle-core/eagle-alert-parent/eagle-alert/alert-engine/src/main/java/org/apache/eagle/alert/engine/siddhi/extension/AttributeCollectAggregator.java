/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.alert.engine.siddhi.extension;

import com.google.common.collect.ImmutableList;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.selector.attribute.aggregator.AttributeAggregatorExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.Attribute.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * @since Apr 1, 2016.
 */
@Extension(
        name = "collect",
        namespace = "eagle",
        description = "Collects values in to a list, and returns the list.",
        parameters = {
                @Parameter(name = "value",
                        description = "The value that need to be collected.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"value"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Collects and return values in a list",
                type = {DataType.OBJECT}),
        examples = @Example(
                syntax = "eagle:collect(hosts)",
                description = "Collects and return all hosts."
        )
)
public class AttributeCollectAggregator
        extends AttributeAggregatorExecutor<AttributeCollectAggregator.AggregatorState> {

    private static final Logger LOG = LoggerFactory.getLogger(AttributeCollectAggregator.class);

    /**
     * The initialization method for AttributeAggregatorExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link AttributeCollectAggregator} configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<AggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                 ProcessingMode processingMode,
                                                 boolean outputExpectsExpiredEvents,
                                                 ConfigReader configReader,
                                                 SiddhiQueryContext siddhiQueryContext) {
        // TODO: Support max of elements?
        return AggregatorState::new;
    }

    @Override
    public Object processAdd(Object data, AggregatorState state) {
        state.value.add(data);
        if (LOG.isDebugEnabled()) {
            LOG.debug("processAdd: current values are : " + state.value);
        }
        return ImmutableList.copyOf(state.value);
    }

    @Override
    public Object processAdd(Object[] data, AggregatorState state) {
        state.value.add(data);
        if (LOG.isDebugEnabled()) {
            LOG.debug("processAdd: current values are : " + state.value);
        }
        return ImmutableList.copyOf(state.value);
    }

    // / NOTICE: non O(1)
    @Override
    public Object processRemove(Object data, AggregatorState state) {
        state.value.remove(data);
        if (LOG.isDebugEnabled()) {
            LOG.debug("processRemove: current values are : " + state.value);
        }
        return ImmutableList.copyOf(state.value);
    }

    // / NOTICE: non O(1)
    @Override
    public Object processRemove(Object[] data, AggregatorState state) {
        state.value.remove(data);
        LOG.info("processRemove: current values are : " + state.value);
        return ImmutableList.copyOf(state.value);
    }

    @Override
    public Object reset(AggregatorState state) {
        state.value.clear();
        return state.value;
    }

    @Override
    public Type getReturnType() {
        return Attribute.Type.OBJECT;
    }

    class AggregatorState extends State {
        private LinkedList<Object> value = new LinkedList<>();

        @Override
        public boolean canDestroy() {
            return value.isEmpty();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("valueList", value);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            value = (LinkedList<Object>) state.get("valueList");
        }
    }

}
