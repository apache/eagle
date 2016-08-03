package org.apache.eagle.security.hbase;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.AbstractApplication;
import org.apache.eagle.app.ApplicationContext;
import org.apache.eagle.security.topo.NewKafkaSourcedSpoutProvider;
import storm.kafka.StringScheme;
import storm.kafka.bolt.KafkaBolt;

/**
 * Since 7/27/16.
 */
public class HBaseAuditLogApplication extends AbstractApplication{
    public final static String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public final static String PARSER_TASK_NUM = "topology.numOfParserTasks";
    public final static String JOIN_TASK_NUM = "topology.numOfJoinTasks";
    public final static String SINK_TASK_NUM = "topology.numOfSinkTasks";
    @Override
    protected void buildApp(TopologyBuilder builder, ApplicationContext context) {
        System.setProperty("config.resource", "/application.conf");
        Config config = ConfigFactory.load();
        NewKafkaSourcedSpoutProvider provider = new NewKafkaSourcedSpoutProvider();
        IRichSpout spout = provider.getSpout(config);

        HBaseAuditLogParserBolt bolt = new HBaseAuditLogParserBolt();

        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfParserTasks = config.getInt(PARSER_TASK_NUM);
        int numOfJoinTasks = config.getInt(JOIN_TASK_NUM);
        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);

        builder.setSpout("ingest", spout, numOfSpoutTasks);
        BoltDeclarer boltDeclarer = builder.setBolt("parserBolt", bolt, numOfParserTasks);
        boltDeclarer.fieldsGrouping("ingest", new Fields(StringScheme.STRING_SCHEME_KEY));

        HbaseResourceSensitivityDataJoinBolt joinBolt = new HbaseResourceSensitivityDataJoinBolt(config);
        BoltDeclarer joinBoltDeclarer = builder.setBolt("joinBolt", joinBolt, numOfJoinTasks);
        joinBoltDeclarer.fieldsGrouping("parserBolt", new Fields("f1"));

        KafkaBolt kafkaBolt = new KafkaBolt();
        BoltDeclarer kafkaBoltDeclarer = builder.setBolt("kafkaSink", kafkaBolt, numOfSinkTasks);
        kafkaBoltDeclarer.shuffleGrouping("joinBolt");
    }
}
