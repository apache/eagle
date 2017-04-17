package org.apache.eagle.security.oozie.parse;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka8.Kafka8IO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.eagle.app.BeamApplication;
import org.apache.eagle.app.environment.impl.BeamEnviroment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;

public class OozieAuditLogBeamApplication extends BeamApplication {
  private static Logger LOG = LoggerFactory.getLogger(OozieAuditLogBeamApplication.class);
    private static final String DEFAULT_CONFIG_PREFIX = "dataSourceConfig";
    private static final String DEFAULT_CONSUMER_GROUP_ID = "eagleConsumer";
    private static final String DEFAULT_TRANSACTION_ZK_ROOT = "/consumers";

    private String configPrefix = DEFAULT_CONFIG_PREFIX;
    private SparkPipelineResult res;

    @Override
    public Pipeline execute(Config config, BeamEnviroment environment) {

        Config context = config;
        if (this.configPrefix != null) {
            context = config.getConfig(configPrefix);
        }

        Map<String, String> consumerProps = ImmutableMap.<String, String>of(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest"
                //"partition.assignment.strategy", "range",
                //"group.id", context.hasPath("consumerGroupId") ? context.getString("consumerGroupId") : DEFAULT_CONSUMER_GROUP_ID
        );
        // Kafka topic
        String topic = context.getString("topic");
        // Kafka broker zk connection
        String zkConnString = context.getString("zkConnection");

        // Kafka sink broker zk connection
        String sinkBrokerList = config.getString("dataSinkConfig.brokerList");
        String sinkTopic = config.getString("dataSinkConfig.topic");

        Kafka8IO.Read<String, String> read = Kafka8IO.<String, String>read()
                .withBootstrapServers(zkConnString)
                .withTopics(Collections.singletonList(topic))
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(StringUtf8Coder.of())
                .updateKafkaClusterProperties(consumerProps);

        SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
      Duration batchIntervalDuration = Duration.standardSeconds(5);
      // provide a generous enough batch-interval to have everything fit in one micro-batch.
      options.setBatchIntervalMillis(batchIntervalDuration.getMillis());
      // provide a very generous read time bound, we rely on num records bound here.
      options.setMinReadTimeMillis(batchIntervalDuration.minus(1).getMillis());
      // bound the read on the number of messages - 2 topics of 4 messages each.
      options.setMaxRecordsPerBatch(8L);
        options.setRunner(SparkRunner.class);
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, Map<String, String>>> deduped =
                p.apply(read.withoutMetadata())
                        .apply(ParDo.of(new ExtractLogFn()));

        deduped.apply(Kafka8IO.<String, Map<String, String>>write()
                .withBootstrapServers(sinkBrokerList)
                .withTopic(sinkTopic)
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .updateProducerProperties(ImmutableMap.of("bootstrap.servers", sinkBrokerList))
        );

        return p;
    }

    public SparkPipelineResult getRes() {
        return res;
    }

    public void setRes(SparkPipelineResult res) {
        this.res = res;
    }

    private static class ExtractLogFn extends DoFn<KV<String, String>, KV<String, Map<String, String>>> {

        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            String log = c.element().getValue();
            LOG.info("--------------log "+log);
            Map<String, String> map = (Map<String, String>) new OozieAuditLogKafkaDeserializer().deserialize(log.getBytes("UTF-8"));
            c.output(KV.of("f1", map));
        }
    }
}
