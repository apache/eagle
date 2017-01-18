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

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Result {
    //for EagleStorePublisher
    private TaggedLogAPIEntity alertEntity = null;//TODO
    //for EmailPublisher
    private Map<String, List<ProcessorResult>> alertMessages = new HashMap<>();

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
            normalizeResult(processorResult);
            alertMessages.get(typeName).add(processorResult);
        }
    }

    public TaggedLogAPIEntity getAlertEntity() {
        return alertEntity;
    }

    public Map<String, List<ProcessorResult>> getAlertMessages() {
        return alertMessages;
    }

    private void normalizeResult(ProcessorResult processorResult) {
        if (processorResult.getSettings() != null && !processorResult.getSettings().isEmpty()) {
            processorResult.setSuggestion(StringUtils.join(processorResult.getSettings(), "\n"));
        }
    }

    /**
     * Processor result.
     */

    public enum ResultLevel {
        NONE,
        INFO,
        NOTICE,
        WARNING,
        CRITICAL
    }

    public static class ProcessorResult {
        private ResultLevel resultLevel;
        private String message;
        private List<String> settings;
        private String suggestion;

        public ProcessorResult(ResultLevel resultLevel, String message, List<String> settings) {
            this.resultLevel = resultLevel;
            this.message = message;
            this.settings = settings;
        }

        public ProcessorResult(ResultLevel resultLevel, String message) {
            this.resultLevel = resultLevel;
            this.message = message;
            this.settings = new ArrayList<>();
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

        public List<String> getSettings() {
            return settings;
        }

        public void setSettings(List<String> settings) {
            this.settings = settings;
        }

        public String getSuggestion() {
            return suggestion;
        }

        public void setSuggestion(String suggestion) {
            this.suggestion = suggestion;
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
