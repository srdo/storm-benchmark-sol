package storm.benchmark.benchmarks;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

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
