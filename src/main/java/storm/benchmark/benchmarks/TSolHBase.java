package storm.benchmark.benchmarks;


import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.MultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Durability;

import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.spout.RandomMessageSpout;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.util.BenchmarkUtils;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TSolHBase  extends StormBenchmark {
  public static final String TOPOLOGY_LEVEL = "topology.level";
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String BOLT_ID = "bolt";
  public static final String BOLT_NUM = "component.bolt_num";
  public static final String BATCH_SIZE = "batch.size";

  public static final String SHUFFLE = "topology.shuffle";
  public static final int DEFAULT_SHUFFLE = 1; // 0 = disabled; other = enabled
  public static final String KAFKA_TOPIC = "kafka.topic";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_BOLT_NUM = 4;
  public static final int DEFAULT_BATCH_SIZE = 100;

  private static final String FIELD1_NAME = "id";
  private static final String FIELD2_NAME = "bytes";



  @Override
  public StormTopology getTopology(Config config) {
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    final int batchSize = BenchmarkUtils.getInt(config, BATCH_SIZE, DEFAULT_BATCH_SIZE);
    final int shuffle = BenchmarkUtils.getInt(config, SHUFFLE, DEFAULT_SHUFFLE);
    final String topic = BenchmarkUtils.getStr(config, KAFKA_TOPIC);

    // 1 -  Setup Trident Kafka Spout   --------
    String zkConnString = "cn108-10.l42scl.hortonworks.com:2181";

    BrokerHosts zk = new ZkHosts(zkConnString);
    TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topic);
    spoutConf.scheme = new SimpleSchemehb();
    spoutConf.ignoreZkOffsets = true;
    spoutConf.fetchSizeBytes = batchSize;
    OpaqueTridentKafkaSpout kspout = new OpaqueTridentKafkaSpout(spoutConf);



    // 2 -  Setup Trident HBase Bolt   --------
    SimpleTridentHBaseMapper hbaseMapper = new SimpleTridentHBaseMapper()
            .withColumnFamily("cf1")
            .withRowKeyField(FIELD1_NAME)
            .withColumnFields(new Fields(FIELD2_NAME))
              ;

    Map<String, Object> hbConf = new HashMap<String, Object>();
    hbConf.put("hbase.rootdir", "hdfs://cn108-10.l42scl.hortonworks.com:8020/apps/hbase/data");
    hbConf.put("hbase.zookeeper.property.clientPort", "2181");
    hbConf.put("hbase.zookeeper.quorum","cn106-10.l42scl.hortonworks.com,cn107-10.l42scl.hortonworks.com,cn108-10.l42scl.hortonworks.com");
    hbConf.put("hbase.zookeeper.useMulti","true");
    config.put("hbase.conf", hbConf);


    String hbaseTable = "stormperf";
    HBaseState.Options options = new HBaseState.Options()
            .withConfigKey("hbase.conf")
            .withDurability(Durability.SYNC_WAL)
            .withMapper(hbaseMapper)
            .withTableName(hbaseTable);

    StateFactory factory = new HBaseStateFactory(options);



    // 3 - Setup Topology  --------
    TridentTopology trident = new TridentTopology();


    Stream strm = trident.newStream("spout", kspout).groupBy()
            .parallelismHint(spoutNum);

    if(shuffle!=0) {
      strm.shuffle();
    } else {
      boltNum = spoutNum; //override if not shuffling
    }

    strm.partitionPersist(factory, new Fields(FIELD1_NAME, FIELD2_NAME), new HBaseUpdater(), new Fields())
          .parallelismHint(boltNum);

    return trident.build();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(IMetricsCollector.MetricsItem.ALL));
  }
}

class SimpleSchemehb implements MultiScheme {

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