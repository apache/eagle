# Hadoop Jmx Collector

These scripts help to collect Hadoop jmx and evently sent the metrics to stdout or Kafka. 

## How to use it
  
  1. edit the configuration file `cronus.ini` (json file)
     
  2. run the scripts
  
        # for general use
        python hadoop_jmx_kafka.py > 1.txt
        # for eBay cronus agent
        python cronus_hadoop_jmx.py
      
## Edit cronus.ini

* input: "port" defines the hadoop service port, such as 50070 => "namenode", 60010 => "hbase master".

* filter: "monitoring.group.selected" can filter out beans which we care about. 

* output: if we left it empty, then the output is stdout by default. 

        "output": {}
        
  It also supports Kafka as its output. 

        "output": {
          "kafka": {
            "topic": "apollo-phx_cronus_nn_jmx",
            "brokerList": [ "druid-test-host1-556191.slc01.dev.ebayc3.com:9092",
                            "druid-test-host2-550753.slc01.dev.ebayc3.com:9092",
                            "druid-test-host3-550755.slc01.dev.ebayc3.com:9092"]
          }
        }
      
## Example
        {
           "env": {
            "site": "apollo-phx",
            "cluster": "apollo",
            "datacenter": "phx"
           },
           "input": {
            "port": "50070",
            "https": false
           },
           "filter": {
            "monitoring.group.selected": ["hadoop"]
           },
           "output": {
           }
        }

