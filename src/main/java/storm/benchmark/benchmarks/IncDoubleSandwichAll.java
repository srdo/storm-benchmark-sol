package storm.benchmark.benchmarks;

import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;

import java.util.Map;
import java.util.Random;


public class IncDoubleSandwichAll extends StormBenchmark {

  public static final String SPOUT_ID = "spout";


  @Override
  public StormTopology getTopology(Config config) {
//    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
//    final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    TopologyBuilder builder = new TopologyBuilder();

    // setup spout
    IRichSpout spout = new SandwichedSpout();
    builder.setSpout(SPOUT_ID, spout, 1);

    return builder.createTopology();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(IMetricsCollector.MetricsItem.ALL));
  }


  public static class SandwichedSpout extends BaseRichSpout {

    private static final long serialVersionUID = -4100642374496292646L;
    public static final String FIELDS = "number";

    private long messageCount = 0;
    private SpoutOutputCollector collector = null;
    private Random rand = null;


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      this.rand = new Random();
      this.collector = collector;
    }


    @Override
    public void nextTuple() {
      long val = rand.nextInt(Integer.MAX_VALUE)+1;
      collector.emit(new Values( val*2 ), messageCount);
      messageCount++;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS));
    }
  }
}
