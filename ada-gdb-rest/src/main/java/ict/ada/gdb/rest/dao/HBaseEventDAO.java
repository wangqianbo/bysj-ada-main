package ict.ada.gdb.rest.dao;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Event;
import ict.ada.common.model.RelationType;
import ict.ada.common.model.WdeRef;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.schema.GdbHTableConstant;

import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseEventDAO {
  private static Log LOG = LogFactory.getLog(HBaseEventDAO.class);
  private GdbRestHTablePool pool;

  public HBaseEventDAO(GdbRestHTablePool pool) {
    this.pool = pool;
  }

  /**
   * @param event
   * @param timeRange
   *          TimeRange should in hour level and range is [start , end)
   * @return
   * @throws GdbException
   */
  public Event getEventWdeRefs(Event event, TimeRange timeRange) throws GdbException {
    byte[] start = null;
    byte[] end = null;
    HTableInterface hiEventDocRelTable = pool.getEventDocRelTable();
    try {
      byte[] id = Bytes.toBytes(event.getId());
      if (!timeRange.equals(TimeRange.ANY_TIME)) {
        start = Bytes.toBytes(getTsRangeStart((int) timeRange.getStartInclusiveInSec()));
        end = Bytes.toBytes(getTsRangeStart((int) timeRange.getEndExclusiveInSec()));
      } else {
        start = Bytes.toBytes(0);
        end = Bytes.toBytes(Integer.MAX_VALUE);
      }
      Scan scan = new Scan();
      scan.setStartRow(Bytes.add(id, start));
      scan.setStopRow(Bytes.add(id, end));// exclusive
      scan.setCaching(10000);
      ResultScanner rs = hiEventDocRelTable.getScanner(scan);
      for (Result result : rs) {
        WdeRef wdeRef = new WdeRef(Bytes.tail(result.getRow(), 8), 0, 0);
        event.addWdeRef(wdeRef);
      }
      return event;
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiEventDocRelTable);
    }
  }

  private int getTsRangeStart(int ts) {
    return ts <= 0 ? 0 : ts - ts % GdbHTableConstant.TIME_GRANULARITY;
  }

  private byte[] getEventTailId(byte[] relationId) {
    return Arrays.copyOfRange(relationId, 8, 12);
  }

  private RelationType getRelationType(byte[] relationId) {
    return RelationType.getType(Bytes.toString(Bytes.tail(relationId, relationId.length
        - Edge.EDGEID_SIZE)));
  }

  /**
   * Close an HTable and log the Exception if any.
   */
  private void closeHTable(HTableInterface htable) {
    if (htable == null) return;
    try {
      htable.close();
    } catch (IOException e) {
      LOG.error("Fail to close HTable: " + Bytes.toString(htable.getTableName()), e);
    }
  }
}
