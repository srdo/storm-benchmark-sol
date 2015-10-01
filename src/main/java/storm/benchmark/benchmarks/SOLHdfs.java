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
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
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

public class SOLHdfs extends StormBenchmark {

  public static final String SPOUT_ID = "spout2";
  public static final String BOLT_ID = "hdfsbolt2";

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

    final int syncCount = BenchmarkUtils.getInt(config, "syncCount", 10000);

    kafka.api.OffsetRequest.EarliestTime();


    // 1 -  Setup Kafka Spout   --------
    String zkHost = BenchmarkUtils.getStr(config, "zookeeper.host");
    String zkConnString = zkHost + ":2181";
    //String zkConnString = "cn069.l42scl.hortonworks.com:2181";
    // String topicName = "parts_1_100b";
    String topicName = BenchmarkUtils.getStr(config, "kafka.topic");
    String namenode_host = BenchmarkUtils.getStr(config, "hdfs_namenode.host");

    BrokerHosts hosts = new ZkHosts(zkConnString);
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
    spoutConfig.scheme = new SimpleScheme();
    spoutConfig.ignoreZkOffsets = true;

    spout = new KafkaSpout(spoutConfig);

    // 2 -  Setup HFS Bolt   --------

    RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");
    SyncPolicy syncPolicy = new CountSyncPolicy(syncCount);
    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.GB);

    // Use default, Storm-generated file names
    long now = System.currentTimeMillis();
    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/stormperf/" + now);
   
    String Hdfs_url = "hdfs://" + namenode_host + ":8020";

    // Instantiate the HdfsBolt
    HdfsBolt bolt = new HdfsBolt()
            .withFsUrl(Hdfs_url)
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(format)
            .withRotationPolicy(rotationPolicy)
            .withSyncPolicy(syncPolicy);
  
    //HdfsBolt bolt = new HdfsBolt()
    //        .withFsUrl("hdfs://cn069.l42scl.hortonworks.com:8020")
    //        .withFileNameFormat(fileNameFormat)
    //        .withRecordFormat(format)
    //        .withRotationPolicy(rotationPolicy)
    //        .withSyncPolicy(syncPolicy);



    // 3 - Setup Topology  --------

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(BOLT_ID, bolt, boltNum)
            .shuffleGrouping(SPOUT_ID);

    return builder.createTopology();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(MetricsItem.ALL));
  }
}
