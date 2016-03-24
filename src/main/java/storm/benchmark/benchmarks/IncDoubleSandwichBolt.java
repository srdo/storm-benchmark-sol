package storm.benchmark.benchmarks;

import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.spout.RandomNumberSpout;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;

import java.util.Map;


public class IncDoubleSandwichBolt extends StormBenchmark {

//  public static final String TOPOLOGY_LEVEL = "topology.level";
//  public static final String SPOUT_NUM = "component.spout_num";
//  public static final String BOLT_NUM = "component.bolt_num";
  public static final String SPOUT_ID = "spout";
  public static final String BOLT_ID1 = "bolt1";
  public static final String BOLT_ID2 = "bolt2";

//  public static final int DEFAULT_SPOUT_NUM = 4;
//  public static final int DEFAULT_BOLT_NUM = 4;

  @Override
  public StormTopology getTopology(Config config) {
//    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
//    final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    TopologyBuilder builder = new TopologyBuilder();
    // setup spout
    IRichSpout spout = new RandomNumberSpout();
    builder.setSpout(SPOUT_ID, spout, 1);

//    // setup bolt
    builder.setBolt(BOLT_ID1, new IncrementDoubleBolt(), 1)
            .localOrShuffleGrouping(SPOUT_ID);
    return builder.createTopology();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(IMetricsCollector.MetricsItem.ALL));
  }


  public static class IncrementDoubleBolt extends BaseRichBolt {
    private static final long serialVersionUID = -5313598399155365865L;
    public static final String FIELDS = "number";
    private OutputCollector collector;


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      Long val = (Long) tuple.getValue(0);
      collector.emit(new Values( 2*(val+1)) );  // increment and double value
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS));
    }
  }
}