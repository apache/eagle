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
package org.apache.eagle.alert.engine.siddhi.extension;

import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

import com.google.common.collect.ImmutableList;

public class AttributeCollectWithDistinctAggregator extends AttributeAggregator {

    private static final Logger LOG = LoggerFactory.getLogger(AttributeCollectAggregator.class);

    private LinkedList<Object> value;

    public AttributeCollectWithDistinctAggregator() {
        value = new LinkedList<Object>();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public Object[] currentState() {
        return value.toArray();
    }

    @Override
    public void restoreState(Object[] arg0) {
        value = new LinkedList<Object>();
        if (arg0 != null) {
            for (Object o : arg0) {
                value.add(o);
            }
        }
    }

    @Override
    public Type getReturnType() {
        return Attribute.Type.OBJECT;
    }

    @Override
    protected void init(ExpressionExecutor[] arg0, ExecutionPlanContext arg1) {
        // TODO: Support max of elements?
    }

    @Override
    public Object processAdd(Object arg0) {
    	// AttributeAggregator.process is already synchronized
    	if (value.contains(arg0)) {
    		value.remove(arg0);
    	}
    	value.add(arg0);
        if (LOG.isDebugEnabled()) {
            LOG.debug("processAdd: current values are : " + value);
        }
        return ImmutableList.copyOf(value);
    }

    @Override
    public Object processAdd(Object[] arg0) {
    	// AttributeAggregator.process is already synchronized
    	if (value.contains(arg0)) {
    		value.remove(arg0);
    	}
    	value.add(arg0);
        if (LOG.isDebugEnabled()) {
            LOG.debug("processAdd: current values are : " + value);
        }
        return ImmutableList.copyOf(value);
    }

    // / NOTICE: non O(1)
    @Override
    public Object processRemove(Object arg0) {
        value.remove(arg0);
        if (LOG.isDebugEnabled()) {
            LOG.debug("processRemove: current values are : " + value);
        }
        return ImmutableList.copyOf(value);
    }

    // / NOTICE: non O(1)
    @Override
    public Object processRemove(Object[] arg0) {
        value.remove(arg0);
        LOG.info("processRemove: current values are : " + value);
        return ImmutableList.copyOf(value);
    }

    @Override
    public Object reset() {
        value.clear();
        return value;
    }
    
}
