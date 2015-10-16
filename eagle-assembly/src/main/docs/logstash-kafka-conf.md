# Logstash-kafka 

### Install logstash-kafka plugin


#### For Logstash 1.5.x


logstash-kafka has been intergrated into [logstash-input-kafka][logstash-input-kafka] and [logstash-output-kafka][logstash-output-kafka], you can directly use it.

[logstash-input-kafka]: https://github.com/logstash-plugins/logstash-input-kafka
[logstash-output-kafka]: https://github.com/logstash-plugins/logstash-output-kafka

#### For Logstash 1.4.x

In logstash 1.4.x, the online version does not support specifying partition\_key for Kafka producer, and data will be produced into each partitions in turn. For eagle, we need to use the src in hdfs\_audit\_log as the partition key, so some hacking work have been done. If you have the same requirment, you can follow it. 

1. Install logstash-kafka

        cd /path/to/logstash
        GEM_HOME=vendor/bundle/jruby/1.9 GEM_PATH= java -jar vendor/jar/jruby-complete-1.7.11.jar -S gem install logstash-kafka
        cp -R vendor/bundle/jruby/1.9/gems/logstash-kafka-*-java/{lib/logstash/*,spec/*} {lib/logstash/,spec/}
        # test install
        USE_JRUBY=1 bin/logstash rspec spec/**/kafka*.rb

    or

        cd /path/to/logstash-kafka
        make tarball
        <!-- a tarball package will be generated under build, including logstash -->

2. Hacking the kafka.rb

   We have added partition\_key\_format, which is used to specify the partition_key and supported by logstash 1.5.x, into  lib/logstash/outputs/kafka.rb. More details are shown [here](https://github.xyz.com/eagle/eagle/blob/master/eagle-assembly/src/main/docs/kafka.rb).
       
          config :partition_key_format, :validate => :string, :default => nil
        
          public
          def register
            LogStash::Logger.setup_log4j(@logger)
        
            options = {
                :broker_list => @broker_list,
                :compression_codec => @compression_codec,
                :compressed_topics => @compressed_topics,
                :request_required_acks => @request_required_acks,
                :serializer_class => @serializer_class,
                :partitioner_class => @partitioner_class,
                :request_timeout_ms => @request_timeout_ms,
                :producer_type => @producer_type,
                :key_serializer_class => @key_serializer_class,
                :message_send_max_retries => @message_send_max_retries,
                :retry_backoff_ms => @retry_backoff_ms,
                :topic_metadata_refresh_interval_ms => @topic_metadata_refresh_interval_ms,
                :queue_buffering_max_ms => @queue_buffering_max_ms,
                :queue_buffering_max_messages => @queue_buffering_max_messages,
                :queue_enqueue_timeout_ms => @queue_enqueue_timeout_ms,
                :batch_num_messages => @batch_num_messages,
                :send_buffer_bytes => @send_buffer_bytes,
                :client_id => @client_id
            }
            @producer = Kafka::Producer.new(options)
            @producer.connect
        
            @logger.info('Registering kafka producer', :topic_id => @topic_id, :broker_list => @broker_list)
        
            @codec.on_event do |data|
              begin
                @producer.send_msg(@current_topic_id,@partition_key,data)
              rescue LogStash::ShutdownSignal
                @logger.info('Kafka producer got shutdown signal')
              rescue => e
                @logger.warn('kafka producer threw exception, restarting',
                             :exception => e)
              end
            end
          end # def register
        
          def receive(event)
            return unless output?(event)
            if event == LogStash::SHUTDOWN
              finished
              return
            end
            @partition_key = if @partition_key_format.nil? then nil else event.sprintf(@partition_key_format) end
            @current_topic_id = if @topic_id.nil? then nil else event.sprintf(@topic_id) end
            @codec.encode(event)
            @partition_key = nil
            @current_topic_id = nil
          end


### Create logstash configuration file
Go to the logstash root dir, and create a configure file

        input {
            file {
                type => "hdp-nn-audit"
                path => "/path/to/audit.log"
                start_position => end
                sincedb_path => "/var/log/logstash/"
             }
        }

        filter{
            if [type] == "hdp-nn-audit" {
        	   grok {
        	       match => ["message", "ugi=(?<user>([\w\d\-]+))@|ugi=(?<user>([\w\d\-]+))/[\w\d\-.]+@|ugi=(?<user>([\w\d.\-_]+))[\s(]+"]
        	   }
            }
        }

        output {
            if [type] == "hdp-nn-audit" {
                kafka {
                    codec => plain {
                        format => "%{message}"
                    }
                    broker_list => "localhost:9092"
                    topic_id => "hdfs_audit_log"
                    request_required_acks => 0
                    request_timeout_ms => 10000
                    producer_type => "async"
                    message_send_max_retries => 3
                    retry_backoff_ms => 100
                    queue_buffering_max_ms => 5000
                    queue_enqueue_timeout_ms => 5000
                    batch_num_messages => 200
                    send_buffer_bytes => 102400
                    client_id => "hdp-nn-audit"
                    partition_key_format => "%{user}"
                }
                # stdout { codec => rubydebug }
            }
        }

#### grok pattern testing
We have 3 typical patterns for ugi field as follows
2015-02-11 15:00:00,000 INFO FSNamesystem.audit: allowed=true	ugi=user1@xyz.com (auth:TOKEN)	ip=/10.115.44.55	cmd=open	src=/apps/hdmi-technology/b_pulsar_coe/schema/avroschema/Session.avsc	dst=null	perm=null
2015-02-11 15:00:00,000 INFO FSNamesystem.audit: allowed=true	ugi=hdc_uc4_platform (auth:TOKEN) via sg_adm@xyz.com (auth:TOKEN)	ip=/10.115.11.54	cmd=open	src=/sys/soj/event/2015/02/08/same_day/00000000000772509716119204458864#3632400774990000-949461-r-01459.avro	dst=null	perm=null

### Reference Links
1. [logstash-kafka](https://github.com/joekiller/logstash-kafka)
2. [logstash](https://github.com/elastic/logstash)

