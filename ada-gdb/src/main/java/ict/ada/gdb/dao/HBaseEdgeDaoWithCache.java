package ict.ada.gdb.dao;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import ict.ada.common.model.Node;
import ict.ada.common.model.RelationGraph;
import ict.ada.common.util.Timer;
import ict.ada.gdb.cache.CacheFacade;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.RelQuerySpec;

public class HBaseEdgeDaoWithCache extends HBaseEdgeDAO {

  private final CacheFacade gdbCache;

  private final boolean LOAD_CACHE_ON_MISS = false;

  private final AtomicLong relQueryCacheMiss = new AtomicLong();
  private final AtomicLong relatedNodesQueryCacheMiss = new AtomicLong();
  private final AtomicLong relQueryCacheHit = new AtomicLong();
  private final AtomicLong relatedNodesQueryCacheHit = new AtomicLong();

  public HBaseEdgeDaoWithCache(GdbHTablePool pool) {
    super(pool);
    gdbCache = CacheFacade.get();

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        StringBuilder sb = new StringBuilder();
        long relQueryHit = relQueryCacheHit.get(), relQueryMiss = relQueryCacheMiss.get();
        long relatedNodesHit = relatedNodesQueryCacheHit.get(), relatedNodesMiss = relatedNodesQueryCacheMiss
            .get();
        sb.append("=========Cache Statistics========\n");
        sb.append("RelationQuery Hit=" + relQueryHit + " Miss=" + relQueryMiss + "\n");
        sb.append("RelationQuery Hit Rate=" + relQueryHit * 1.0 / (relQueryHit + relQueryMiss)
            + "\n");
        sb.append("RelatedNodesQuery Hit=" + relatedNodesHit + " Miss=" + relatedNodesMiss + "\n");
        sb.append("RelatedNodesQuery Hit Rate=" + relatedNodesHit * 1.0
            / (relatedNodesHit + relatedNodesMiss) + "\n");
        System.out.println(sb.toString());
      }
    }));
  }

  @Override
  public RelationGraph queryRelationGraph(RelQuerySpec spec) throws GdbException {
    if (spec.isCritical()) {
      // queries marked 'critical' will never use GdbCache
      return super.queryRelationGraph(spec);
    }
    long start = Timer.now();
    RelationGraph resultInCache = gdbCache.queryRelationGraph(spec, LOAD_CACHE_ON_MISS);
    if (resultInCache == null) {
      System.out.println("Cache miss overhead: " + Timer.msSince(start) + "ms");
      relQueryCacheMiss.incrementAndGet();
      // fall back to HBase access
      return super.queryRelationGraph(spec);
    } else {
      relQueryCacheHit.incrementAndGet();
      return resultInCache;
    }
  }

  @Override
  protected List<byte[]> getRelatedNodeIdList(Node nodeToExpand, PathQuerySpec spec)
      throws Exception {
    List<byte[]> relatedIdList = gdbCache.queryRelatedNodeIdList(nodeToExpand, spec);
    if (relatedIdList == null) {
      relatedNodesQueryCacheMiss.incrementAndGet();
      return super.getRelatedNodeIdList(nodeToExpand, spec);
    } else {
      relatedNodesQueryCacheHit.incrementAndGet();
      // System.out.println("=+=" + relatedIdList.size());
      return relatedIdList;
    }
  }

}
