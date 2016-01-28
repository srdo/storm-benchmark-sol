package storm.benchmark.benchmarks;

import org.apache.storm.tuple.Values;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Created by rnaik on 7/10/15.
 */
class Const extends BaseFunction {
  private static final long serialVersionUID = -8605358179216331897L;

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    collector.emit(new Values(tuple.getValue(0)));
  }
}
