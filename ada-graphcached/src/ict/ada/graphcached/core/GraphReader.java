/**
 * 
 */
package ict.ada.graphcached.core;

import ict.ada.graphcached.model.CachedNode;
import ict.ada.graphcached.model.CachedRelationship;
import ict.ada.graphcached.util.Pair;
import ict.ada.graphcached.util.Triplet;

import java.util.List;
import java.util.Map;

/**
 * @author forhappy
 *
 */
public interface GraphReader {
  
  public final static String SENTRY_NODEID = "0xdeadbeef"; 
  
  public List<Triplet<CachedNode, CachedNode, CachedRelationship>> read(String file);
  
  public Map<CachedNode, List<Pair<CachedNode, CachedRelationship>>> read2(String file);
}
