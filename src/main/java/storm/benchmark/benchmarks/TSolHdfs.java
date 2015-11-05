package storm.benchmark.benchmarks;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.MultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Sets;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
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
import java.util.List;
import java.util.UUID;


public class TSolHdfs  extends StormBenchmark {
  public static final String TOPOLOGY_LEVEL = "topology.level";
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String BOLT_ID = "bolt";
  public static final String BOLT_NUM = "component.bolt_num";
  public static final String BATCH_SIZE = "batch.size";
  // public static final String KAFKA_TOPIC = "kafka.topic";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_BOLT_NUM = 4;
  public static final int DEFAULT_BATCH_SIZE = 100;
  private static final String FIELD1_NAME = "id";
  private static final String FIELD2_NAME = "bytes";

  public static final String SHUFFLE = "topology.shuffle";
  public static final int DEFAULT_SHUFFLE = 1; // 0 = disabled; other = enabled


  @Override
  public StormTopology getTopology(Config config) {
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    final int batchSize = BenchmarkUtils.getInt(config, BATCH_SIZE, DEFAULT_BATCH_SIZE);
    final int shuffle = BenchmarkUtils.getInt(config, SHUFFLE, DEFAULT_SHUFFLE);
    // final String topic = BenchmarkUtils.getStr(config, KAFKA_TOPIC);
    // 1 -  Setup Trident Kafka Spout   --------
    
    String zkHost = BenchmarkUtils.getStr(config, "zookeeper.host");
    String zkConnString = zkHost + ":2181";
    //String zkConnString = "cn069.l42scl.hortonworks.com:2181";
    String topicName = BenchmarkUtils.getStr(config, "kafka.topic");
    // String topicName = "parts_4_100b";
    
    String namenode_host = BenchmarkUtils.getStr(config, "hdfs_namenode.host");

    BrokerHosts zk = new ZkHosts(zkConnString);
    TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topicName);
    spoutConf.scheme = new SimpleHFSScheme();
    spoutConf.ignoreZkOffsets = true;
    spoutConf.fetchSizeBytes = batchSize;
    OpaqueTridentKafkaSpout kspout = new OpaqueTridentKafkaSpout(spoutConf);


    // 2 -  Setup Trident Hdfs Bolt   --------
    Fields hdfsFields = new Fields(FIELD1_NAME, FIELD2_NAME);

    FileNameFormat fileNameFormat = new DefaultFileNameFormat()
            .withPrefix("trident")
            .withExtension(".txt")
            .withPath("/tmp/stormperf/");

    RecordFormat recordFormat = new DelimitedRecordFormat()
            .withFields(hdfsFields);

    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(500.0f, FileSizeRotationPolicy.Units.MB);

    String Hdfs_url = "hdfs://" + namenode_host + ":8020";

    //HdfsState.Options options = new HdfsState.HdfsFileOptions()
    //        .withFileNameFormat(fileNameFormat)
    //        .withRecordFormat(recordFormat)
    //        .withRotationPolicy(rotationPolicy)
    //        .withFsUrl("hdfs://cn069.l42scl.hortonworks.com:8020");
    
    HdfsState.Options options = new HdfsState.HdfsFileOptions()
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(recordFormat)
            .withRotationPolicy(rotationPolicy)
            .withFsUrl(Hdfs_url);	    


    StateFactory factory = new HdfsStateFactory().withOptions(options);


    // 3 - Setup Topology  --------
    TridentTopology trident = new TridentTopology();

    Stream strm = trident.newStream("spout", kspout);

    if(shuffle!=0) {
	strm.parallelismHint(spoutNum).shuffle().partitionPersist(factory, new Fields(FIELD1_NAME, FIELD2_NAME), new HdfsUpdater(), new Fields()).parallelismHint(boltNum);
    } else {
	strm.parallelismHint(spoutNum).shuffle().partitionPersist(factory, new Fields(FIELD1_NAME, FIELD2_NAME), new HdfsUpdater(), new Fields()).parallelismHint(spoutNum);
    }
     
    
    //strm.parallelismHint(boltNum);
    return trident.build();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(IMetricsCollector.MetricsItem.ALL));
  }
}


class SimpleHFSScheme implements MultiScheme {

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
