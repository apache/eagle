package org.apache.eagle.jpm.mr.history.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.*;

import org.apache.eagle.jpm.mr.history.common.JHFConfigManager;
import org.apache.eagle.jpm.mr.history.entities.JobProcessTimeStampEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryJobProgressBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HistoryJobProgressBolt.class);

    private final static int MAX_RETRY_TIMES = 3;
    private Long m_minTimeStamp;
    private int m_numTotalPartitions;
    private JHFConfigManager configManager;
    private Map<Integer, Long> m_partitionTimeStamp = new TreeMap<>();
    public HistoryJobProgressBolt(String parentName, JHFConfigManager configManager) {
        this.configManager = configManager;
        m_numTotalPartitions = this.configManager.getConfig().getInt("envContextConfig.parallelismConfig." + parentName);
        m_minTimeStamp = 0L;
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        Integer partitionId = tuple.getIntegerByField("partitionId");
        Long timeStamp = tuple.getLongByField("timeStamp");
        LOG.info("partition " + partitionId + ", timeStamp " + timeStamp);
        if (!m_partitionTimeStamp.containsKey(partitionId) || (m_partitionTimeStamp.containsKey(partitionId) && m_partitionTimeStamp.get(partitionId) < timeStamp)) {
            m_partitionTimeStamp.put(partitionId, timeStamp);
        }

        if (m_partitionTimeStamp.size() >= m_numTotalPartitions) {
            //get min timestamp
            Long minTimeStamp = Collections.min(m_partitionTimeStamp.values());

            if (m_minTimeStamp == 0L) {
                m_minTimeStamp = minTimeStamp;
            }

            if (m_minTimeStamp > minTimeStamp) {
                //no need to update
                return;
            }

            m_minTimeStamp = minTimeStamp;
            final JHFConfigManager.EagleServiceConfig eagleServiceConfig = configManager.getEagleServiceConfig();
            final JHFConfigManager.JobExtractorConfig jobExtractorConfig = configManager.getJobExtractorConfig();
            Map<String, String> baseTags = new HashMap<String, String>() { {
                put("site", jobExtractorConfig.site);
            } };
            JobProcessTimeStampEntity entity = new JobProcessTimeStampEntity();
            entity.setCurrentTimeStamp(m_minTimeStamp);
            entity.setTimestamp(m_minTimeStamp);
            entity.setTags(baseTags);

            IEagleServiceClient client = new EagleServiceClientImpl(
                    eagleServiceConfig.eagleServiceHost,
                    eagleServiceConfig.eagleServicePort,
                    eagleServiceConfig.username,
                    eagleServiceConfig.password);

            client.getJerseyClient().setReadTimeout(jobExtractorConfig.readTimeoutSeconds * 1000);

            List<JobProcessTimeStampEntity> entities = new ArrayList<>();
            entities.add(entity);

            int tried = 0;
            while (tried <= MAX_RETRY_TIMES) {
                try {
                    LOG.info("start flushing JobProcessTimeStampEntity entities of total number " + entities.size());
                    client.create(entities);
                    LOG.info("finish flushing entities of total number " + entities.size());
                    break;
                } catch (Exception ex) {
                    if (tried < MAX_RETRY_TIMES) {
                        LOG.error("Got exception to flush, retry as " + (tried + 1) + " times", ex);
                    } else {
                        LOG.error("Got exception to flush, reach max retry times " + MAX_RETRY_TIMES, ex);
                    }
                }
                tried ++;
            }

            client.getJerseyClient().destroy();
            try {
                client.close();
            } catch (Exception e) {
                LOG.error("failed to close eagle service client ", e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    @Override
    public void cleanup() {
        super.cleanup();
    }
}
