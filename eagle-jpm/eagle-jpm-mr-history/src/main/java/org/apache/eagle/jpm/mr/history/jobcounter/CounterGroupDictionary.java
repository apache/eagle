/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.history.jobcounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * MR Job counter dictionary. It's singlton class that will try to read JobCounter.conf file and configure
 * counters.
 *
 */
public final class CounterGroupDictionary {

    private final List<CounterGroupKey> groupKeys = new ArrayList<>();

    private static volatile CounterGroupDictionary instance = null;
    private static final Logger LOG = LoggerFactory.getLogger(CounterGroupDictionary.class);

    private CounterGroupDictionary() {}

    public static CounterGroupDictionary getInstance() throws JobCounterException {
        if (instance == null) {
            synchronized (CounterGroupDictionary.class) {
                if (instance == null) {
                    CounterGroupDictionary tmp = new CounterGroupDictionary();
                    tmp.initialize();
                    instance = tmp;
                }
            }
        }
        return instance;
    }

    public CounterGroupKey getCounterGroupByName(String groupName) {
        for (CounterGroupKey groupKey : groupKeys) {
            if (groupKey.getName().equalsIgnoreCase(groupName)) {
                return groupKey;
            }
        }
        return null;
    }

    public CounterGroupKey getCounterGroupByIndex(int groupIndex) {
        if (groupIndex < 0 || groupIndex >= groupKeys.size()) {
            return null;
        }
        return groupKeys.get(groupIndex);
    }

    private void initialize() throws JobCounterException {
        // load config.properties file from classpath
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("/JobCounter.conf");
        try {
            if (is == null) {
                is = this.getClass().getClassLoader().getResourceAsStream("JobCounter.conf");
                if (is == null) {
                    final String errMsg = "Failed to load JobCounter.conf";
                    LOG.error(errMsg);
                    throw new JobCounterException(errMsg);
                }
            }
            final Properties prop = new Properties();
            try {
                prop.load(is);
            } catch(Exception ex) {
                final String errMsg = "Failed to load JobCounter.conf, reason: " + ex.getMessage();
                LOG.error(errMsg, ex);
                throw new JobCounterException(errMsg, ex);
            }
            int groupIndex = 0;
            while (parseGroup(groupIndex, prop)) {
                ++groupIndex;
            }
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private boolean parseGroup(int groupIndex, Properties prop) {
        final String groupKeyBase = "counter.group" + groupIndex;
        final String groupNameKey = groupKeyBase + ".name";
        final String groupName = prop.getProperty(groupNameKey);

        if (groupName == null) {
            return false;
        }

        final String groupDescriptionKey = groupKeyBase + ".description";
        final String groupDescription = prop.getProperty(groupDescriptionKey);
        final CounterGroupKeyImpl groupKey = new CounterGroupKeyImpl(groupIndex, groupName, groupDescription);
        final ArrayList<CounterKey> counters = new ArrayList<CounterKey>();

        int counterIndex = 0;
        while (parseCounter(groupKey, counterIndex, counters, prop)) {
            ++counterIndex;
        }
        groupKey.setCounterKeys(counters.toArray(new CounterKey[counters.size()]));
        groupKeys.add(groupKey);
        return true;
    }

    private boolean parseCounter(CounterGroupKey groupKey, int counterIndex, List<CounterKey> counters, Properties prop) {
        final String counterKeyBase = "counter.group" + groupKey.getIndex() + ".counter" + counterIndex;
        final String counterNameKey = counterKeyBase + ".names";
        final String counterNamesString = prop.getProperty(counterNameKey);

        if (counterNamesString == null) {
            return false;
        }
        final String[] names = counterNamesString.split(",");
        final List<String> counterNames = new ArrayList<String>();
        for (String name : names) {
            counterNames.add(name.trim());
        }

        final String counterDescriptionKey = counterKeyBase + ".description";
        final String counterDescription = prop.getProperty(counterDescriptionKey);

        CounterKey counter = new CounterKeyImpl(counterIndex, counterNames, counterDescription, groupKey);
        counters.add(counter);
        return true;
    }

    private static class CounterKeyImpl implements CounterKey {
        private final int index;
        private final List<String> counterNames;
        private final String description;
        private final CounterGroupKey groupKey;

        public CounterKeyImpl(int index, List<String> counterNames, String description, CounterGroupKey groupKey) {
            this.index = index;
            this.counterNames = counterNames;
            this.description = description;
            this.groupKey = groupKey;
        }
        @Override
        public int getIndex() {
            return index;
        }
        @Override
        public List<String> getNames() {
            return counterNames;
        }
        @Override
        public String getDescription() {
            return description;
        }
        @Override
        public CounterGroupKey getGroupKey() {
            return groupKey;
        }
    }

    private static class CounterGroupKeyImpl implements CounterGroupKey {
        private final int index;
        private final String name;
        private final String description;
        private CounterKey[] counterKeys;

        public CounterGroupKeyImpl(int index, String name, String description) {
            this.index = index;
            this.name = name;
            this.description = description;
        }

        public void setCounterKeys(CounterKey[] counterKeys) {
            this.counterKeys = counterKeys;
        }

        @Override
        public int getIndex() {
            return index;
        }
        @Override
        public String getName() {
            return name;
        }
        @Override
        public String getDescription() {
            return description;
        }
        @Override
        public int getCounterNumber() {
            return counterKeys.length;
        }
        @Override
        public List<CounterKey> listCounterKeys() {
            return Arrays.asList(counterKeys);
        }
        @Override
        public CounterKey getCounterKeyByName(String name) {
            for (CounterKey counterKey : counterKeys) {
                for (String n : counterKey.getNames()) {
                    if (n.equalsIgnoreCase(name)) {
                        return counterKey;
                    }
                }
            }
            return null;
        }
        @Override
        public CounterKey getCounterKeyByID(int index) {
            if (index < 0 || index >= counterKeys.length) {
                return null;
            }
            return counterKeys[index];
        }
    }
}
