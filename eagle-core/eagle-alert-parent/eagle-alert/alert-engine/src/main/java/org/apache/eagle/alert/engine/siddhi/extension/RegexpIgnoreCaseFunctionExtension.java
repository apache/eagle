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
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * Tells whether or not this 'string' matches the given regular expression 'regex'.
 * Accept Type(s): (STRING,STRING)
 * Return Type(s): BOOLEAN
 */
@Extension(
        name = "regexpIgnoreCase",
        namespace = "str",
        description = "Returns whether 'source' string matches the given regular expression 'regex'.",
        parameters = {
                @Parameter(name = "source",
                        description = "Source string.",
                        type = {DataType.STRING},
                        dynamic = true),
                @Parameter(name = "regex",
                        description = "Regex string.",
                        type = {DataType.STRING},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"source", "regex"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns whether 'source' matches the given regular expression 'regex'.",
                type = {DataType.BOOL}),
        examples = {
                @Example(
                        syntax = "str:regexpIgnoreCase(string, regex)",
                        description = "Returns whether 'source' matches the given regular expression 'regex'.")
        }
)
public class RegexpIgnoreCaseFunctionExtension extends FunctionExecutor {

    //state-variables
    private boolean isRegexConstant = false;
    private Pattern patternConstant;

    /**
     * The initialization method for EqualsIgnoreCaseExtension,
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
            throw new SiddhiAppValidationException("Invalid no of arguments passed to str:regexpIgnoreCase() function, "
                    + "required 2, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the first argument of "
                    + "str:regexpIgnoreCase() function, required " + Attribute.Type.STRING + ", but found "
                    + attributeExpressionExecutors[0].getReturnType().toString());
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the second argument of "
                    + "str:regexpIgnoreCase() function, required " + Attribute.Type.STRING + ", but found "
                    + attributeExpressionExecutors[1].getReturnType().toString());
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            String regexConstant = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            patternConstant = Pattern.compile(regexConstant, Pattern.CASE_INSENSITIVE);
            isRegexConstant = true;
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        String regex;
        Pattern pattern;
        Matcher matcher;

        if (data[0] == null) {
            throw new SiddhiAppRuntimeException("Invalid input given to str:regexpIgnoreCase() function. "
                    + "First argument cannot be null");
        }
        if (data[1] == null) {
            throw new SiddhiAppRuntimeException("Invalid input given to str:regexpIgnoreCase() function. "
                    + "Second argument cannot be null");
        }
        String source = (String) data[0];

        if (!isRegexConstant) {
            regex = (String) data[1];
            pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
            matcher = pattern.matcher(source);
            return matcher.matches();

        } else {
            matcher = patternConstant.matcher(source);
            return matcher.matches();
        }
    }

    @Override
    protected Object execute(Object data, State state) {
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.BOOL;
    }

}
