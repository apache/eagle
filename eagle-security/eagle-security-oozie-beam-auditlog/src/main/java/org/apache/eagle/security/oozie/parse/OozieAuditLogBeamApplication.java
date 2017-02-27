package org.apache.eagle.security.oozie.parse;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.eagle.app.BeamApplication;
import org.apache.eagle.app.environment.impl.BeamEnviroment;
import org.joda.time.Duration;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;

public class OozieAuditLogBeamApplication extends BeamApplication {

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

        Map<String, Object> consumerProps = ImmutableMap.of(
                "auto.offset.reset", "earliest",
                //"partition.assignment.strategy", "range",
                "group.id", context.hasPath("consumerGroupId") ? context.getString("consumerGroupId") : DEFAULT_CONSUMER_GROUP_ID
        );
        // Kafka topic
        String topic = context.getString("topic");
        // Kafka broker zk connection
        String zkConnString = context.getString("zkConnection");

        // Kafka sink broker zk connection
        String sinkBrokerList = config.getString("dataSinkConfig.brokerList");
        String sinkTopic = config.getString("dataSinkConfig.topic");
        Duration batchIntervalDuration = Duration.standardSeconds(5);

        KafkaIO.Read<String, String> read = KafkaIO.<String, String>read()
                .withBootstrapServers(zkConnString)
                .withTopics(Arrays.asList(topic))
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(StringUtf8Coder.of())
                .updateConsumerProperties(consumerProps);

        SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
        options.setRunner(SparkRunner.class);
        options.setCheckpointDir("/tmp");
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, Map<String, String>>> deduped =
                p.apply(read.withoutMetadata()).setCoder(
                        KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                        .apply(Window.<KV<String, String>>into(FixedWindows.of(batchIntervalDuration)))
                        .apply(Distinct.create())
                        .apply(ParDo.of(new ExtractLogFn()));

        deduped.apply(KafkaIO.<String, Map<String, String>>write()
                .withBootstrapServers(sinkBrokerList)
                .withTopic(sinkTopic)
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
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
            Map<String, String> map = (Map<String, String>) new OozieAuditLogKafkaDeserializer().deserialize(log.getBytes("UTF-8"));
            c.output(KV.of("f1", map));
        }
    }
}
