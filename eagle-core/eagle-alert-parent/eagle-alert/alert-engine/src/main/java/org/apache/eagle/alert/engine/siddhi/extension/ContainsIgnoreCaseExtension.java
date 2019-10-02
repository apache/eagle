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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

@Extension(
        name = "containsIgnoreCase",
        namespace = "str",
        description = "Returns whether a string contains another given string.",
        parameters = {
                @Parameter(name = "first.string",
                        description = "Source string.",
                        type = {DataType.STRING},
                        dynamic = true),
                @Parameter(name = "second.string",
                        description = "Regex string.",
                        type = {DataType.STRING},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"first.string", "second.string"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns whether 'first.string' contains 'second.string'.",
                type = {DataType.BOOL}),
        examples = {
                @Example(
                        syntax = "str:containsIgnoreCase(stringA, stringB) as isContains",
                        description = "Returns whether 'stringA' contains 'stringB'.")
        }
)
public class ContainsIgnoreCaseExtension extends FunctionExecutor {

    /**
     * The initialization method for ContainsIgnoreCaseExtension,
     * this method will be called before the other methods.
     *
     * @param attributeExpressionExecutors the executors of each function parameter
     * @param configReader                 the config reader for the Siddhi app
     * @param siddhiQueryContext           the context of the Siddhi query
     */
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to str:containsIgnoreCase() "
                    + "function, required 2, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the first argument of "
                    + "str:containsIgnoreCase() function, required " + Attribute.Type.STRING + ", but found "
                    + attributeExpressionExecutors[0].getReturnType().toString());
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the second argument of "
                    + "str:containsIgnoreCase() function, required " + Attribute.Type.STRING + ", but found "
                    + attributeExpressionExecutors[1].getReturnType().toString());
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        if (data[0] == null) {
            throw new SiddhiAppRuntimeException("Invalid input given to str:containsIgnoreCase() function. "
                    + "First argument cannot be null");
        }
        if (data[1] == null) {
            throw new SiddhiAppRuntimeException("Invalid input given to str:containsIgnoreCase() function. "
                    + "Second argument cannot be null");
        }
        String str1 = (String) data[0];
        String str2 = (String) data[1];
        return str1.toUpperCase().contains(str2.toUpperCase());
    }

    @Override
    protected Object execute(Object data, State state) {
        // Since the containsIgnoreCase function takes in 2 parameters,
        // this method does not get called. Hence, not implemented.
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.BOOL;
    }

}
