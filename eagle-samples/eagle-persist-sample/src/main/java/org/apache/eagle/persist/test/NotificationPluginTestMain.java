package org.apache.eagle.persist.test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created on 2/16/16.
 */
public class NotificationPluginTestMain {
    public static void main(String[] args){
        System.setProperty("config.resource", "/application-plugintest.conf");
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm();
        env.fromSpout(createProvider(env.getConfig())).withOutputFields(2).nameAs("testSpout").alertWithConsumer("testStream", "testExecutor");
        env.execute();
    }

    public static StormSpoutProvider createProvider(Config config) {
        return new StormSpoutProvider(){

            @Override
            public BaseRichSpout getSpout(Config context) {
                return new TestSpout();
            }
        };
    }

    public static class TestSpout extends BaseRichSpout {
        private static final Logger LOG = LoggerFactory.getLogger(TestSpout.class);
        private SpoutOutputCollector collector;
        public TestSpout() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(5000);
            LOG.info("emitted tuple ...");
            Map<String, Object> map = new TreeMap<>();
            map.put("testAttribute", "testValue");
            collector.emit(new Values("testStream", map));
        }
    }
}
