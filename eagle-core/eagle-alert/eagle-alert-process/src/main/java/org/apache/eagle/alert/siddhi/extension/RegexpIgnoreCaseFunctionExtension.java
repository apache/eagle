/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.siddhi.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.extension.string.RegexpFunctionExtension;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * regexpIgnoreCase(string, regex)
 * Tells whether or not this 'string' matches the given regular expression 'regex'.
 * Accept Type(s): (STRING,STRING)
 * Return Type(s): BOOLEAN
 */
public class RegexpIgnoreCaseFunctionExtension extends RegexpFunctionExtension {

    //state-variables
    boolean isRegexConstant = false;
    String regexConstant;
    Pattern patternConstant;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to str:regexpIgnoreCase() function, required 2, " +
                    "but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the first argument of str:regexpIgnoreCase() function, " +
                    "required "+ Attribute.Type.STRING+", but found "+attributeExpressionExecutors[0].getReturnType().toString());
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the second argument of str:regexpIgnoreCase() function, " +
                    "required "+ Attribute.Type.STRING+", but found "+attributeExpressionExecutors[1].getReturnType().toString());
        }
        if(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor){
            isRegexConstant = true;
            regexConstant = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            patternConstant = Pattern.compile(regexConstant, Pattern.CASE_INSENSITIVE);
        }
    }

    @Override
    protected Object execute(Object[] data) {
        String regex;
        Pattern pattern;
        Matcher matcher;

        if (data[0] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to str:regexpIgnoreCase() function. First argument cannot be null");
        }
        if (data[1] == null) {
            throw new ExecutionPlanRuntimeException("Invalid input given to str:regexpIgnoreCase() function. Second argument cannot be null");
        }
        String source = (String) data[0];

        if(!isRegexConstant){
            regex = (String) data[1];
            pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
            matcher = pattern.matcher(source);
            return matcher.matches();

        } else {
            matcher = patternConstant.matcher(source);
            return matcher.matches();
        }
    }
}
