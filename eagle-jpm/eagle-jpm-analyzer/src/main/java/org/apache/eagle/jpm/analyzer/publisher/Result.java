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
    private Map<String, List<TaggedLogAPIEntity>> alertEntities = new HashMap<>();
    //for EmailPublisher
    private Map<String, List<ProcessorResult>> alertMessages = new HashMap<>();

    public void addEvaluatorResult(Class<?> type, EvaluatorResult result) {
        Map<Class<?>, ProcessorResult> processorResults = result.getProcessorResults();
        Map<Class<?>, TaggedLogAPIEntity> processorEntities = result.getProcessorEntities();

        for (Class<?> processorType : processorResults.keySet()) {
            ProcessorResult processorResult = processorResults.get(processorType);

            if (processorResult.resultLevel.equals(ResultLevel.NONE)) {
                continue;
            }

            TaggedLogAPIEntity entity = processorEntities.get(processorType);

            String typeName = type.getName();
            if (!alertMessages.containsKey(typeName)) {
                alertMessages.put(typeName, new ArrayList<>());
                alertEntities.put(typeName, new ArrayList<>());
            }
            normalizeResult(processorResult);
            alertMessages.get(typeName).add(processorResult);
            alertEntities.get(typeName).add(entity);

        }
    }

    public Map<String, List<ProcessorResult>> getAlertMessages() {
        return alertMessages;
    }

    public Map<String, List<TaggedLogAPIEntity>> getAlertEntities() {
        return alertEntities;
    }

    private void normalizeResult(ProcessorResult processorResult) {
        if (processorResult.getSettings() != null && !processorResult.getSettings().isEmpty()) {
            processorResult.setSettingList(StringUtils.join(processorResult.getSettings(), "\n"));
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

    public enum RuleType {
        COMPRESS,
        SPLIT,
        SPILL,
        TASK_NUMBER,
        GC_TIME,
        RESOURCE_CONTENTION,
        DATA_SKEW,

        LONG_STUCK_JOB,
        LONG_DURATION_JOB
    }

    public static class ProcessorResult {
        private RuleType ruleType;
        private ResultLevel resultLevel;
        private String message;
        private List<String> settings;
        private String settingList;

        public ProcessorResult(RuleType ruleType, ResultLevel resultLevel, String message, List<String> settings) {
            this.ruleType = ruleType;
            this.resultLevel = resultLevel;
            this.message = message;
            this.settings = settings;
        }

        public ProcessorResult(RuleType ruleType, ResultLevel resultLevel, String message) {
            this.ruleType = ruleType;
            this.resultLevel = resultLevel;
            this.message = message;
            this.settings = new ArrayList<>();
        }

        public RuleType getRuleType() {
            return ruleType;
        }

        public void setRuleType(RuleType ruleType) {
            this.ruleType = ruleType;
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

        public String getSettingList() {
            return settingList;
        }

        public void setSettingList(String settingList) {
            this.settingList = settingList;
        }
    }

    /**
     * Evaluator result.
     */
    public static class EvaluatorResult {
        private Map<Class<?>, ProcessorResult> processorResults = new HashMap<>();
        private Map<Class<?>, TaggedLogAPIEntity> processorEntities = new HashMap<>();

        public void addProcessorResult(Class<?> type, ProcessorResult result) {
            this.processorResults.put(type, result);
        }

        public void addProcessorEntity(Class<?> type, TaggedLogAPIEntity entity) {
            this.processorEntities.put(type, entity);
        }

        public Map<Class<?>, ProcessorResult> getProcessorResults() {
            return this.processorResults;
        }

        public Map<Class<?>, TaggedLogAPIEntity> getProcessorEntities() {
            return processorEntities;
        }
    }
}
