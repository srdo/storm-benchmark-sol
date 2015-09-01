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

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.Sets;
import storm.benchmark.lib.spout.RandomMessageSpout;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;

import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


import storm.trident.spout.IBatchSpout;
import backtype.storm.task.TopologyContext;

import static storm.benchmark.metrics.IMetricsCollector.MetricsItem;


public class TSOL extends StormBenchmark {

  public static final String TOPOLOGY_LEVEL = "topology.level";
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String BOLT_ID = "bolt";
  public static final String BOLT_NUM = "component.bolt_num";
  public static final String BATCH_SIZE = "batch.size";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_BOLT_NUM = 4;
  public static final int DEFAULT_BATCH_SIZE = 100;

  private IBatchSpout spout;

  @Override
  public StormTopology getTopology(Config config) {
    final int msgSize = BenchmarkUtils.getInt(config, RandomMessageSpout.MESSAGE_SIZE,
            RandomMessageSpout.DEFAULT_MESSAGE_SIZE);
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);
    final int batchSize = BenchmarkUtils.getInt(config, BATCH_SIZE, DEFAULT_BATCH_SIZE);

    spout = new BatchSpout(msgSize, batchSize);

    TridentTopology trident = new TridentTopology();

    trident.newStream("spout", spout).name("message").parallelismHint(spoutNum)
            .shuffle()
            .each(new Fields("message"), new Const(), new Fields("word1"))
            .parallelismHint(boltNum)
            .each(new Fields("word1"), new Const(), new Fields("word2"))
            .parallelismHint(boltNum)
    ;
    return trident.build();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(MetricsItem.ALL));
  }

}






 class BatchSpout implements IBatchSpout {

  private static final long serialVersionUID = -4100642374492292646L;
  public static final String FIELDS = "message";

  private int batchSize;
  private int eventSize;

  private String [] messages = null;
  private Random rand = null;
  HashMap<Long, List<String>> batches = new HashMap<Long, List<String>>();
  private int index = 0;

  public BatchSpout(int eventSize, int batchSize) {
    this.eventSize = eventSize;
    this.batchSize = batchSize;
  }

  public void open(Map conf, TopologyContext context) {
    this.rand = new Random();
    final int differentMessages = 100;
    this.messages = new String[differentMessages];
    for(int i = 0; i < differentMessages; i++) {
      StringBuilder sb = new StringBuilder(eventSize);
      for(int j = 0; j < eventSize; j++) {
        sb.append(rand.nextInt(9));
      }
      messages[i] = sb.toString();
    }
  }

  @Override
  public void emitBatch(long batchId, TridentCollector collector) {
    List<String> batch = this.batches.get(batchId);
    if(batch == null) {
      batch = new ArrayList<String>();
      for(int i=0; i<batchSize; i++) {
        batch.add( messages[index] );
        index++;
        if(index>=batchSize) {
          index = 0;
        }
      }
      this.batches.put(batchId, batch);
    }

    for(String event : batch) {
      collector.emit(new Values(event));
    }
  }

  @Override
  public void ack(long batchId) {
    batches.remove(batchId);
  }

  @Override
  public void close() {
    // nothing to do here
  }

  @Override
  public Map getComponentConfiguration() {
    // no particular configuration here
    return new Config();
  }

  @Override
  public Fields getOutputFields() {
    return new Fields(FIELDS);
  }
}