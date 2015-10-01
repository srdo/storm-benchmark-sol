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
 * limitations under the License
 */

package storm.benchmark.benchmarks;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.MultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Sets;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;
import org.apache.storm.hbase.bolt.*;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static storm.benchmark.metrics.IMetricsCollector.MetricsItem;

/**
 * forked from https://github.com/yahoo/storm-perf-test
 */

public class SOLHBase extends StormBenchmark {

  public static final String SPOUT_ID = "spout";
  public static final String BOLT_ID = "hbasebolt";

  private static final String FIELD1_NAME = "id";
  private static final String FIELD2_NAME = "bytes";

  public static final String SPOUT_NUM = "component.spout_num";
  public static final String BOLT_NUM = "component.bolt_num";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_BOLT_NUM = 4;

  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    kafka.api.OffsetRequest.EarliestTime();

    // 1 -  Setup Spout   --------

    String zkHost = BenchmarkUtils.getStr(config, "zookeeper.host");
    String zkConnString = zkHost + ":2181";
    //String zkConnString = "cn069.l42scl.hortonworks.com:2181";
    String topicName = BenchmarkUtils.getStr(config, "kafka.topic");
    // String topicName = "parts_4_100b";
    String hbase_table  = BenchmarkUtils.getStr(config, "hbase_table.name");
    String hbase_table_column = BenchmarkUtils.getStr(config, "hbase_table_column.name");	
    String namenode_host = BenchmarkUtils.getStr(config, "hdfs_namenode.host"); 
    String zookeeper_znode_parent = BenchmarkUtils.getStr(config, "zookeeper.znode.parent");

    BrokerHosts hosts = new ZkHosts(zkConnString);
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
    spoutConfig.scheme = new SimpleScheme();
    spoutConfig.ignoreZkOffsets = true;

    spout = new KafkaSpout(spoutConfig);

    // 2 -  Setup Bolt   --------
    //SimpleHBaseMapper mapper = new SimpleHBaseMapper()
    //        .withRowKeyField(FIELD1_NAME)
    //        .withColumnFields(new Fields(FIELD2_NAME))
    //        .withColumnFamily("cf1");

    SimpleHBaseMapper mapper = new SimpleHBaseMapper()
            .withRowKeyField(FIELD1_NAME)
            .withColumnFields(new Fields(FIELD2_NAME))
            .withColumnFamily(hbase_table_column);

    //HBaseBolt hbolt = new HBaseBolt("stormperf", mapper)
    //                         .withConfigKey("hbase.conf");

    HBaseBolt hbolt = new HBaseBolt(hbase_table, mapper)
                             .withConfigKey("hbase.conf");

    String hbase_root_dir_value = "hdfs://" + namenode_host + ":8020/apps/hbase/data";
    
    Map<String, Object> hbConf = new HashMap<String, Object>();
    hbConf.put("hbase.rootdir", hbase_root_dir_value);
    hbConf.put("hbase.zookeeper.property.clientPort", "2181");
    hbConf.put("hbase.zookeeper.quorum",zkHost);
    hbConf.put("zookeeper.znode.parent", zookeeper_znode_parent);
    hbConf.put("hbase.zookeeper.useMulti","true");

    config.put("hbase.conf", hbConf);

    // 3 - Setup Topology  --------

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(BOLT_ID, hbolt, boltNum)
            .shuffleGrouping(SPOUT_ID);

    return builder.createTopology();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(MetricsItem.ALL));
  }
}

class SimpleScheme implements MultiScheme {

  public Iterable<List<Object>> deserialize(byte[] bytes)  {
    ArrayList<List<Object>> res = new ArrayList(1);
    res.add(tuple(UUID.randomUUID().toString(), deserializeString(bytes)) );
    return res;
  }

  public static String deserializeString(byte[] string) {
    try {
      return new String(string, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public Fields getOutputFields() {
    return new Fields("id", "bytes");
  }

  public static ArrayList<Object> tuple(Object... values) {
    ArrayList<Object> arr = new ArrayList<Object>(values.length);
    for (Object value : values) {
      arr.add(value);
    }
    return arr;
  }

}
