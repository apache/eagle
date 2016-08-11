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

package org.apache.eagle.jpm.mr.history.parser;

import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.entities.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.jpm.mr.history.parser.JHFMRVer1Parser.Keys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.util.CountersStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Listener holds all informations related to one whole job history file, so it's stateful and does not support multithreading.
 * @author yonzhang
 *
 */
public class JHFMRVer1EventReader extends JHFEventReaderBase implements JHFMRVer1PerLineListener {
    private static final Logger logger = LoggerFactory.getLogger(JHFMRVer1EventReader.class);

    /**
     * baseTags stores the basic tag name values which might be used for persisting various entities
     * baseTags includes: cluster, datacenter and jobName
     * baseTags are used for all job/task related entities
     * @param baseTags
     */
    public JHFMRVer1EventReader(Map<String, String> baseTags, Configuration configuration, JobHistoryContentFilter filter) {
        super(baseTags, configuration, filter);
    }

    @Override
    public void handle(RecordTypes recType, Map<Keys, String> values)
          throws Exception {
        switch (recType) {
            case Job:
                handleJob(null, values, values.get(Keys.COUNTERS));
                break;
            case Task:
                handleTask(RecordTypes.Task, null, values, values.get(Keys.COUNTERS));
                break;
            case MapAttempt:
                ensureRackHostnameAfterAttemptFinish(values);
                handleTask(RecordTypes.MapAttempt, null, values, values.get(Keys.COUNTERS));
                break;
            case ReduceAttempt:
                ensureRackHostnameAfterAttemptFinish(values);
                handleTask(RecordTypes.ReduceAttempt, null, values, values.get(Keys.COUNTERS));
                break;
            default:
                // skip other types
                ;
        }
    }
     
    private void ensureRackHostnameAfterAttemptFinish(Map<Keys, String> values) {
        // only care about attempt finish
        if (values.get(Keys.FINISH_TIME) == null)
            return;
        String hostname = null;
        String rack = null;
        // we get rack/hostname based on task's status
        if (values.get(Keys.TASK_STATUS).equals(EagleTaskStatus.SUCCESS.name())) {
            // in CDH4.1.4, the hostname has the format of /default/rack128/some.server.address
            // if not specified, the hostname has the format  of /default-rack/<hostname>
            String decoratedHostname = values.get(Keys.HOSTNAME);
            String[] tmp = decoratedHostname.split("/");
            hostname = tmp[tmp.length - 1];
            rack = tmp[tmp.length - 2];
            m_host2RackMapping.put(hostname, rack);
        } else if(values.get(Keys.TASK_STATUS).equals(EagleTaskStatus.KILLED.name()) || values.get(Keys.TASK_STATUS).equals(EagleTaskStatus.FAILED.name())) {
            hostname = values.get(Keys.HOSTNAME);
            // make every effort to get RACK information
            hostname = (hostname == null) ? "" : hostname;
            rack = m_host2RackMapping.get(hostname);
        }
          
        values.put(Keys.HOSTNAME, hostname);
        values.put(Keys.RACK, rack);
    }
    
    @Override
    protected JobCounters parseCounters(Object value) throws IOException {
        JobCounters jc = new JobCounters();
        Map<String, Map<String, Long>> groups = new HashMap<String, Map<String, Long>>();
        Counters counters = new Counters(); 
        try {
            CountersStrings.parseEscapedCompactString((String)value, counters);
        } catch (Exception ex) {
            logger.error("can not parse job history", ex);
            throw new IOException(ex);
        }
        Iterator<CounterGroup> it = counters.iterator();
        while (it.hasNext()) {
            CounterGroup cg = it.next();

           // hardcoded to exclude business level counters
            if (!cg.getName().equals("org.apache.hadoop.mapreduce.FileSystemCounter")
                && !cg.getName().equals("org.apache.hadoop.mapreduce.TaskCounter")
                && !cg.getName().equals("org.apache.hadoop.mapreduce.JobCounter")
                && !cg.getName().equals("org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter")
                && !cg.getName().equals("org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter")
                && !cg.getName().equals("FileSystemCounters")                                      // for artemis
                && !cg.getName().equals("org.apache.hadoop.mapred.Task$Counter")                   // for artemis
                && !cg.getName().equals("org.apache.hadoop.mapreduce.lib.input.FileInputFormat$Counter")  // for artemis
                && !cg.getName().equals("org.apache.hadoop.mapreduce.lib.input.FileOutputFormat$Counter")
            ) continue;

            groups.put(cg.getName(), new HashMap<String, Long>());
            Map<String, Long> counterValues = groups.get(cg.getName());
            logger.debug("groupname:" + cg.getName() + "(" + cg.getDisplayName() + ")");
            Iterator<Counter> iterCounter = cg.iterator();
            while (iterCounter.hasNext()) {
                Counter c = iterCounter.next();
                counterValues.put(c.getName(), c.getValue());
                logger.debug(c.getName() + "=" + c.getValue() + "(" + c.getDisplayName() + ")");
            }
        }
        jc.setCounters(groups);
        return jc;
    }
    
    public JobExecutionAPIEntity jobExecution() {
        return m_jobExecutionEntity;
    }
}
