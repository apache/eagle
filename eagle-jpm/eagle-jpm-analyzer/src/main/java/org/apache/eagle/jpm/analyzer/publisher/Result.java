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

package org.apache.eagle.jpm.analyzer.publisher;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Result {
    //for EagleStorePublisher
    private TaggedLogAPIEntity alertEntity = null;//TODO
    //for EmailPublisher
    private Map<String, List<Pair<ResultLevel, String>>> alertMessages = new HashMap<>();

    public void addEvaluatorResult(Class<?> type, EvaluatorResult result) {
        Map<Class<?>, ProcessorResult> processorResults = result.getProcessorResults();
        for (Class<?> processorType : processorResults.keySet()) {
            ProcessorResult processorResult = processorResults.get(processorType);
            if (processorResult.resultLevel.equals(ResultLevel.NONE)) {
                continue;
            }

            String typeName = type.getName();
            if (!alertMessages.containsKey(typeName)) {
                alertMessages.put(typeName, new ArrayList<>());
            }
            alertMessages.get(typeName).add(Pair.of(processorResult.getResultLevel(), processorResult.getMessage()));
        }
    }

    public TaggedLogAPIEntity getAlertEntity() {
        return alertEntity;
    }

    public Map<String, List<Pair<ResultLevel, String>>> getAlertMessages() {
        return alertMessages;
    }

    /**
     * Processor result.
     */

    public enum ResultLevel {
        NONE,
        NOTICE,
        WARNING,
        CRITICAL
    }

    public static class ProcessorResult {
        private ResultLevel resultLevel;
        private String message;

        public ProcessorResult(ResultLevel resultLevel, String message) {
            this.resultLevel = resultLevel;
            this.message = message;
        }

        public ResultLevel getResultLevel() {
            return resultLevel;
        }

        public void setResultLevel(ResultLevel resultLevel) {
            this.resultLevel = resultLevel;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

    /**
     * Evaluator result.
     */
    public static class EvaluatorResult {
        private Map<Class<?>, ProcessorResult> processorResults = new HashMap<>();

        public void addProcessorResult(Class<?> type, ProcessorResult result) {
            this.processorResults.put(type, result);
        }

        public Map<Class<?>, ProcessorResult> getProcessorResults() {
            return this.processorResults;
        }
    }
}
