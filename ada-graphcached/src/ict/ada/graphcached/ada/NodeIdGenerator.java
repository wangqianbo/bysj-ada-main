/**
 * 
 */
package ict.ada.graphcached.ada;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType;
import ict.ada.graphcached.util.Bytes;
import ict.ada.graphcached.util.MD5Digest;

/**
 * @author forhappy
 *
 */
public class NodeIdGenerator {
  static public byte[] generate(Node node) {
    byte[] nodeId = null;
    NodeType type = node.getType();
    String name = node.getName();
    
    byte[] typeBytes = type.getBytesForm();
    byte[] nodeIdBytesPart = MD5Digest.MD5(name);
    
    nodeId = Bytes.add(typeBytes, nodeIdBytesPart);
    node.setId(nodeId);
    
    return nodeId;
  }
}
