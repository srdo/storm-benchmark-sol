package storm.benchmark.lib.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;


public class RandomNumberSpout extends BaseRichSpout {

  private static final long serialVersionUID = -4100642374496292646L;
  public static final String FIELDS = "number";

  private long messageCount = 0;
  private SpoutOutputCollector collector = null;
//  private StringBuffer message = null;
  private Random rand = null;


  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.rand = new Random();
    this.collector = collector;
  }


  @Override
  public void nextTuple() {
    collector.emit(new Values(new Long(rand.nextInt(Integer.MAX_VALUE))), messageCount);
    messageCount++;
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }
}
