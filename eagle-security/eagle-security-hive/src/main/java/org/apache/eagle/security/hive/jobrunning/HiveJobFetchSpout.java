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

package org.apache.eagle.security.hive.jobrunning;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.commons.lang.StringUtils;
import org.apache.eagle.dataproc.impl.storm.ValuesArray;
import org.apache.eagle.jpm.util.*;
import org.apache.eagle.jpm.util.jobrecover.RunningJobManager;
import org.apache.eagle.jpm.util.resourcefetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.connection.URLConnectionUtils;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.MRJob;
import org.apache.eagle.jpm.util.resourcefetch.model.MRJobsWrapper;
import org.apache.eagle.security.hive.config.RunningJobCrawlConfig;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.net.URLConnection;
import java.util.*;

public class HiveJobFetchSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(HiveJobFetchSpout.class);
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private RunningJobCrawlConfig crawlConfig;
    private SpoutOutputCollector collector;
    private JobIdFilter jobFilter;
    private RMResourceFetcher rmResourceFetcher;
    private static final String XML_HTTP_HEADER = "Accept";
    private static final String XML_FORMAT = "application/xml";
    private static final int CONNECTION_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;
    private Long lastFinishAppTime;
    private RunningJobManager runningJobManager;
    private int partitionId;

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public HiveJobFetchSpout(RunningJobCrawlConfig crawlConfig) {
        this.crawlConfig = crawlConfig;
    }

    private int calculatePartitionId(TopologyContext context) {
        int thisGlobalTaskId = context.getThisTaskId();
        String componentName = context.getComponentId(thisGlobalTaskId);
        List<Integer> globalTaskIds = context.getComponentTasks(componentName);
        int index = 0;
        for (Integer id : globalTaskIds) {
            if (id == thisGlobalTaskId) {
                return index;
            }
            index++;
        }
        throw new IllegalStateException();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.rmResourceFetcher = new RMResourceFetcher(crawlConfig.endPointConfig.RMBasePaths);
        this.partitionId = calculatePartitionId(context);
        // sanity verify 0<=partitionId<=numTotalPartitions-1
        if (partitionId < 0 || partitionId > crawlConfig.controlConfig.numTotalPartitions) {
            throw new IllegalStateException("partitionId should be less than numTotalPartitions with partitionId "
                    + partitionId + " and numTotalPartitions " + crawlConfig.controlConfig.numTotalPartitions);
        }
        Class<? extends JobIdPartitioner> partitionerCls = crawlConfig.controlConfig.partitionerCls;
        try {
            this.jobFilter = new JobIdFilterByPartition(partitionerCls.newInstance(),
                    crawlConfig.controlConfig.numTotalPartitions, partitionId);
        } catch (Exception e) {
            LOG.error("failing instantiating job partitioner class " + partitionerCls.getCanonicalName());
            throw new IllegalStateException(e);
        }
        this.collector = collector;
        this.runningJobManager = new RunningJobManager(crawlConfig.zkStateConfig.zkQuorum,
                crawlConfig.zkStateConfig.zkSessionTimeoutMs,
                crawlConfig.zkStateConfig.zkRetryTimes,
                crawlConfig.zkStateConfig.zkRetryInterval,
                crawlConfig.zkStateConfig.zkRoot,
                crawlConfig.zkStateConfig.zkLockPath);
        this.lastFinishAppTime = this.runningJobManager.recoverLastFinishedTime(partitionId);
        if (this.lastFinishAppTime == 0L) {
            this.lastFinishAppTime = Calendar.getInstance().getTimeInMillis() - 24 * 60 * 60000L;//one day ago
            this.runningJobManager.updateLastFinishTime(partitionId, this.lastFinishAppTime);
        }
    }

    @Override
    public void nextTuple() {
        LOG.info("start to fetch job list");
        try {
            List<AppInfo> apps = rmResourceFetcher.getResource(Constants.ResourceType.RUNNING_MR_JOB);
            if (apps == null) {
                apps = new ArrayList<>();
            }
            handleApps(apps, true);

            long fetchTime = Calendar.getInstance().getTimeInMillis();
            if (fetchTime - this.lastFinishAppTime > 60000L) {
                apps = rmResourceFetcher.getResource(Constants.ResourceType.COMPLETE_MR_JOB, Long.toString(this.lastFinishAppTime));
                if (apps == null) {
                    apps = new ArrayList<>();
                }
                handleApps(apps, false);
                this.lastFinishAppTime = fetchTime;
                this.runningJobManager.updateLastFinishTime(partitionId, fetchTime);
            }
        } catch (Exception e) {
            LOG.warn("exception found {}", e);
        } finally {
            //need to be configured
            Utils.sleep(60);
        }
    }

    private void handleApps(List<AppInfo> apps, boolean isRunning) {
        List<MRJob> mrJobs = new ArrayList<>();
        //fetch job config
        if (isRunning) {
            for (AppInfo appInfo : apps) {
                if (!jobFilter.accept(appInfo.getId())) {
                    continue;
                }

                String jobURL = appInfo.getTrackingUrl() + Constants.MR_JOBS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
                InputStream is = null;
                try {
                    is = InputStreamUtils.getInputStream(jobURL, null, Constants.CompressionType.NONE);
                    LOG.info("fetch mr job from {}", jobURL);
                    mrJobs = OBJ_MAPPER.readValue(is, MRJobsWrapper.class).getJobs().getJob();
                } catch (Exception e) {
                    LOG.warn("fetch mr job from {} failed, {}", jobURL, e);
                    continue;
                } finally {
                    Utils.closeInputStream(is);
                }

                if (fetchRunningConfig(appInfo, mrJobs)) {
                    continue;
                }
            }
        }

        if (!isRunning) {
            for (AppInfo appInfo : apps) {
                if (!jobFilter.accept(appInfo.getId())) {
                    continue;
                }
                MRJob mrJob = new MRJob();
                mrJob.setId(appInfo.getId().replace("application_", "job_"));
                mrJobs.add(mrJob);
                fetchFinishedConfig(appInfo, mrJobs);
            }
        }
    }

    private boolean fetchRunningConfig(AppInfo appInfo, List<MRJob> mrJobs) {
        InputStream is = null;
        for (MRJob mrJob : mrJobs) {
            String confURL = appInfo.getTrackingUrl() + Constants.MR_JOBS_URL + "/" + mrJob.getId() + "/" + Constants.MR_CONF_URL + "?" + Constants.ANONYMOUS_PARAMETER;
            try {
                LOG.info("fetch job conf from {}", confURL);
                final URLConnection connection = URLConnectionUtils.getConnection(confURL);
                connection.setRequestProperty(XML_HTTP_HEADER, XML_FORMAT);
                connection.setConnectTimeout(CONNECTION_TIMEOUT);
                connection.setReadTimeout(READ_TIMEOUT);
                is = connection.getInputStream();
                Map<String, String> hiveQueryLog = new HashMap<>();
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document dt = db.parse(is);
                Element element = dt.getDocumentElement();
                NodeList propertyList = element.getElementsByTagName("property");
                int length = propertyList.getLength();
                for (int i = 0; i < length; i++) {
                    Node property = propertyList.item(i);
                    String key = property.getChildNodes().item(0).getTextContent();
                    String value = property.getChildNodes().item(1).getTextContent();
                    hiveQueryLog.put(key, value);
                }

                if (hiveQueryLog.containsKey(Constants.HIVE_QUERY_STRING)) {
                    collector.emit(new ValuesArray(appInfo.getUser(), mrJob.getId(), Constants.ResourceType.JOB_CONFIGURATION, hiveQueryLog), mrJob.getId());
                }
            } catch (Exception e) {
                LOG.warn("fetch job conf from {} failed, {}", confURL, e);
                e.printStackTrace();
                return false;
            } finally {
                Utils.closeInputStream(is);
            }
        }
        return true;
    }

    private boolean fetchFinishedConfig(AppInfo appInfo, List<MRJob> mrJobs) {
        InputStream is = null;
        for (MRJob mrJob : mrJobs) {
            String urlString = crawlConfig.endPointConfig.HSBasePath + "jobhistory/conf/" + mrJob.getId() + "?" + Constants.ANONYMOUS_PARAMETER;
            try {
                LOG.info("fetch job conf from {}", urlString);
                is = InputStreamUtils.getInputStream(urlString, null, Constants.CompressionType.NONE);
                final org.jsoup.nodes.Document doc = Jsoup.parse(is, "UTF-8", urlString);
                doc.outputSettings().prettyPrint(false);
                org.jsoup.select.Elements elements = doc.select("table[id=conf]").select("tbody").select("tr");
                Map<String, String> hiveQueryLog = new HashMap<>();
                Iterator<org.jsoup.nodes.Element> iter = elements.iterator();
                while (iter.hasNext()) {
                    org.jsoup.nodes.Element element = iter.next();
                    org.jsoup.select.Elements tds = element.children();
                    String key = tds.get(0).text();
                    String value = "";
                    org.jsoup.nodes.Element valueElement = tds.get(1);
                    if (Constants.HIVE_QUERY_STRING.equals(key)) {
                        for (org.jsoup.nodes.Node child : valueElement.childNodes()) {
                            if (child instanceof TextNode) {
                                TextNode valueTextNode = (TextNode) child;
                                value = valueTextNode.getWholeText();
                                value = StringUtils.strip(value);
                            }
                        }
                    } else {
                        value = valueElement.text();
                    }
                    hiveQueryLog.put(key, value);
                }
                if (hiveQueryLog.containsKey(Constants.HIVE_QUERY_STRING)) {
                    collector.emit(new ValuesArray(appInfo.getUser(), mrJob.getId(), Constants.ResourceType.JOB_CONFIGURATION, hiveQueryLog), mrJob.getId());
                }
            } catch (Exception e) {
                LOG.warn("fetch job conf from {} failed, {}", urlString, e);
                e.printStackTrace();
                return false;
            } finally {
                Utils.closeInputStream(is);
            }
        }
        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "jobId", "type", "config"));
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
        //process fail over later
    }
}
