package ict.ada.gdb.maptools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeAttribute;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.WdeRef;
import ict.ada.common.model.NodeAttribute.AttrValueInfo;
import ict.ada.common.util.GeoAddress;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.SortedWdeRefSet;
import ict.ada.gdb.schema.LocationNodeTasksHTable;
import ict.ada.gdb.schema.NodeAttributeHTable;
import ict.ada.gdb.schema.NodeNameHTable;

public class MapNodeUpdater {
  
  private void closeHTable(HTableInterface htable) {
    if (htable == null) return;
    try {
      htable.close();
    } catch (IOException e) {
    }
  }
  
  private byte[] getNodeIdByName(String name, NodeType type) throws GdbException {
    HTableInterface hiValueTable = MapNodeUpdaterConfig.getNodeNameTable();
    try {
      Get get = new Get(name.getBytes());
      get.addColumn(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER);
      Result result = hiValueTable.get(get);
      return result.getValue(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER);
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiValueTable);
    }
  }
  
  private boolean putSingleAttributeWithCheck(HTableInterface htableInterface, byte[] attrRowkey,
      byte[] attrName, byte[] attrValue, byte[] oldAttrValue) throws IOException {
    Put putValue = new Put(attrRowkey);
    putValue.add(Bytes.toBytes(NodeAttributeHTable.FAMILY), attrName, attrValue);
    return htableInterface.checkAndPut(attrRowkey, Bytes.toBytes(NodeAttributeHTable.FAMILY),
        attrName, oldAttrValue, putValue);
  }
  
  private byte[] getAttrubiteByAttrName(HTableInterface htableInterface, byte[] attrRowkey,
      byte[] attrName) throws IOException {
    Get get = new Get(attrRowkey);
    get.addColumn(Bytes.toBytes(NodeAttributeHTable.FAMILY), attrName);
    Result result = htableInterface.get(get);
    if (result == null || result.isEmpty()) {
      return null;
    } else {
      return result.getValue(Bytes.toBytes(NodeAttributeHTable.FAMILY), attrName);
    }
  }
  
  private void updateAttributes(HTableInterface htableInterface, byte[] personIdBytes, Node node)
      throws IOException {
    List<NodeAttribute> attributes = node.getAttributes();
    if (attributes == null || attributes.isEmpty()) return;
    for (int i = 0; i < attributes.size(); i++) {
      NodeAttribute attr = attributes.get(i);
      String key = attr.getKey();
      List<AttrValueInfo> listAttrValueInfo = attr.getValues();
      if (listAttrValueInfo.isEmpty()) continue;
      byte[] attrRowkey = Bytes.add(personIdBytes, key.getBytes());
      for (int j = 0; j < listAttrValueInfo.size(); j++) {
        while (true) {
          AttrValueInfo attrValueInfo = listAttrValueInfo.get(j);
          String value = attrValueInfo.getValue();
          List<WdeRef> listWderefs = attrValueInfo.getWdeRefs();
          byte[] attrName = Bytes.toBytes(value);
          byte[] attrOldWdeRef = getAttrubiteByAttrName(htableInterface, attrRowkey, attrName);
          SortedWdeRefSet set = new SortedWdeRefSet(attrOldWdeRef);
          set.add(listWderefs);
          if (putSingleAttributeWithCheck(htableInterface, attrRowkey, attrName, set.getBytes(),
              attrOldWdeRef)) {
            break;
          }
        }
      }
    }
  }
  
  private Node createMapNode(String nodeName, String lat, String lon) {
    Node node = new Node(NodeType.PLACE_ATTR, nodeName);
    List<WdeRef> wderefsLat = new LinkedList<WdeRef>();
    WdeRef wderefLat = new WdeRef("lat".getBytes(), 0, "lat".getBytes().length);
    wderefsLat.add(wderefLat);
    AttrValueInfo attrValueInfoLat = new AttrValueInfo(lat, wderefsLat);
    List<AttrValueInfo> valuesLat = new LinkedList<NodeAttribute.AttrValueInfo>();
    valuesLat.add(attrValueInfoLat);
    NodeAttribute attrLat = new NodeAttribute("lat", valuesLat);
    node.addNodeAttribute(attrLat);
    
    List<WdeRef> wderefsLon = new LinkedList<WdeRef>();
    WdeRef wderefLon = new WdeRef("lon".getBytes(), 0, "lon".getBytes().length);
    wderefsLon.add(wderefLon);
    AttrValueInfo attrValueInfoLon = new AttrValueInfo(lon, wderefsLon);
    List<AttrValueInfo> valuesLon = new LinkedList<NodeAttribute.AttrValueInfo>();
    valuesLon.add(attrValueInfoLon);
    NodeAttribute attrLon = new NodeAttribute("lon", valuesLon);
    node.addNodeAttribute(attrLon);
    
    return node;
  }
  
  private Node buildNode(String nodeName) {
    String[] latLon = GeoAddress.getCoordinate(nodeName);
    return createMapNode(nodeName, latLon[0], latLon[1]);
  }
  
  private void updateStatus(byte[] rowKey) {
    Put putValue = new Put(rowKey);
    putValue.add(Bytes.toBytes(LocationNodeTasksHTable.FAMILY),
        Bytes.toBytes(LocationNodeTasksHTable.QUALIFIER), Bytes.toBytes(true));
    HTableInterface table = MapNodeUpdaterConfig.getNodeTaskTable();
    try {
      table.put(putValue);
    } catch (IOException e) {
    } finally {
      closeHTable(table);
    }
  }
  
  private List<String> scanTasks(String timeRange, int numTasks) {
    List<String> names = new ArrayList<String>();
    String sampleTimestamp = "1368974302";
    String regexComp = timeRange + ".*";
    HTableInterface table = MapNodeUpdaterConfig.getNodeTaskTable();
    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes(LocationNodeTasksHTable.FAMILY),
        Bytes.toBytes(LocationNodeTasksHTable.QUALIFIER));
    Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
        new RegexStringComparator(regexComp));
    scan.setFilter(filter);
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(scan);
      for (Result res : scanner) {
        byte[] rowKey = res.getRow();
        byte[] name = new byte[rowKey.length - sampleTimestamp.length()];
        System.arraycopy(rowKey, sampleTimestamp.length(), name, 0, name.length);
        byte[] status = res.getValue(Bytes.toBytes(LocationNodeTasksHTable.FAMILY),
        Bytes.toBytes(LocationNodeTasksHTable.QUALIFIER));
        if (Bytes.toBoolean(status) == false) {
          names.add(Bytes.toString(name));
          updateStatus(rowKey);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    } finally {
      if (scanner != null) 
        scanner.close();
      closeHTable(table);
    }
    return names;
  }
  
  private void updateMapNodeAttr(String nodeName) {
    Node mapNode = buildNode(nodeName);
    byte[] nodeId = null;
    try {
      nodeId = getNodeIdByName(nodeName, NodeType.PLACE_ATTR);
      updateAttributes(MapNodeUpdaterConfig.getNodeAttrTable(), nodeId, mapNode);
    } catch (GdbException e) {
      e.printStackTrace();
    } catch (IOException e) {
    }
  }
  
  private void doUpdate(String timeRange) {
    List<String> names = scanTasks(timeRange, 0);
    Iterator<String> iter = names.iterator();
    while (iter.hasNext()) {
      String name = iter.next();
      updateMapNodeAttr(name);
    }
    MapNodeUpdaterConfig.mapNodeConf.setLastUpdateTimestamp();
  }
  
  public void update() {
    String lastUpdate = MapNodeUpdaterConfig.mapNodeConf.getLastUpdateTimestamp();
    doUpdate(lastUpdate);
  }
  
}
