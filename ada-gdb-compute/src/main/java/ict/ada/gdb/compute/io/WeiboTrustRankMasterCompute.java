package ict.ada.gdb.compute.io;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master compute associated with {@link TrustRankComputation}.
 * It registers required aggregators.
 */
public  class WeiboTrustRankMasterCompute extends
    DefaultMasterCompute {
	
	 private static String SUM_AGG = "sum";
	  /** Min aggregator name */
	  private static String MIN_AGG = "min";
	  /** Max aggregator name */
	  private static String MAX_AGG = "max";
	
  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerAggregator(SUM_AGG, LongSumAggregator.class);
    registerPersistentAggregator(MIN_AGG, FloatMinAggregator.class);
    registerPersistentAggregator(MAX_AGG, FloatMaxAggregator.class);
  }
}
