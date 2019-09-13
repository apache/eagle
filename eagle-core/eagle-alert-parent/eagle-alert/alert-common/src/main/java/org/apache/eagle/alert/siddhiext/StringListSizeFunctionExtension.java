/*
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

package org.apache.eagle.alert.siddhiext;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.eagle.alert.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension(
        name = "listSize",
        namespace = "str",
        description = "Returns the size of a string list",
        parameters = {
                @Parameter(name = "source.list",
                        description = "Source string (List).",
                        type = {DataType.STRING},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"source.list"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the size of a string list",
                type = {DataType.INT}),
        examples = {
                @Example(
                        syntax = "str:listSize(stringList)",
                        description = "Returns the size of the stringList")
        }
)
public class StringListSizeFunctionExtension extends FunctionExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(StringListSizeFunctionExtension.class);

    /**
     * The initialization method for StringListSizeFunctionExtension,
     * this method will be called before the other methods.
     *
     * @param attributeExpressionExecutors  the executors of each function parameter
     * @param configReader                  the config reader for the Siddhi app
     * @param siddhiQueryContext            the context of the Siddhi query
     */
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to str:listSize() function, "
                    + "required 1, but found " + attributeExpressionExecutors.length);
        }

        Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
        if (attributeType != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the argument of str:listSize() "
                    + "function, required " + Attribute.Type.STRING + ", but found " + attributeType.toString());
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return null;
    }

    @Override
    protected Object execute(Object data, State state) {
        try {
            return JsonUtils.jsonStringToList((String) data).size();
        } catch (Exception e) {
            LOG.warn("exception found {0}", e);
            return 0;
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.INT;
    }

}
