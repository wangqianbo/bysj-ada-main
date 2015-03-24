package ict.ada.gdb.compute.io;


import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.FloatWritable;

/**
 * Aggregator for getting max double value.
 */
public class FloatMaxAggregator extends BasicAggregator<FloatWritable> {
  @Override
  public void aggregate(FloatWritable value) {
    getAggregatedValue().set(
        Math.max(getAggregatedValue().get(), value.get()));
  }

  @Override
  public FloatWritable createInitialValue() {
    return new FloatWritable(Float.NEGATIVE_INFINITY);
  }
}
