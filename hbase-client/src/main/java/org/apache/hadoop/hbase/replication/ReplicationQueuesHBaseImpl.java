/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;

@InterfaceAudience.Private
public class ReplicationQueuesHBaseImpl implements ReplicationQueues {

  /** Name of the HBase Table used for tracking replication*/
  public static final TableName REPLICATION_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "replication");

  // Column family and column names for the Replication Table
  private static final byte[] CF = Bytes.toBytes("r");
  private static final byte[] COL_OWNER = Bytes.toBytes("o");
  private static final byte[] COL_QUEUE_ID = Bytes.toBytes("q");

  // Column Descriptor for the Replication Table
  private static final HColumnDescriptor REPLICATION_COL_DESCRIPTOR =
    new HColumnDescriptor(CF).setMaxVersions(1)
      .setInMemory(true)
      .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        // TODO: Figure out which bloom filter to use
      .setBloomFilterType(BloomType.NONE)
      .setCacheDataInL1(true);

  // Common byte values used in replication offset tracking
  private static final byte[] INITIAL_OFFSET = Bytes.toBytes(0L);
  private static final byte[] NEGATIVE_OFFSET = Bytes.toBytes(-1L);

  /*
   * Make sure that HBase table operations for replication have a high number of retries. This is
   * because the server is aborted if any HBase table operation fails. Each RPC will be attempted
   * 50000 times before exiting. This provides each operation with around 1.25 hours of retries
   * before the server is aborted.
   */
  private static final int CLIENT_RETRIES = 50000;
  private static final int RPC_TIMEOUT = 100;
  private static final int OPERATION_TIMEOUT = CLIENT_RETRIES * RPC_TIMEOUT;

  private Configuration conf = null;
  private Admin admin = null;
  private Connection connection = null;
  private Table replicationTable = null;
  private Abortable abortable = null;
  private String serverName = null;

  public ReplicationQueuesHBaseImpl(ReplicationQueuesArguments args) throws IOException {
    this(args.getConf(), args.getAbort());
  }

  public ReplicationQueuesHBaseImpl(Configuration conf, Abortable abort) throws IOException {
    this.conf = conf;
    // Modify the connection's config so that the Replication Table it returns has a much higher
    // number of client retries
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, CLIENT_RETRIES);
    this.connection = ConnectionFactory.createConnection(conf);
    this.admin = connection.getAdmin();
    this.abortable = abort;
    replicationTable = createAndGetReplicationTable();
    replicationTable.setRpcTimeout(RPC_TIMEOUT);
    replicationTable.setOperationTimeout(OPERATION_TIMEOUT);
  }

  @Override
  public void init(String serverName) throws ReplicationException {
    this.serverName = serverName;
  }

  @Override
  public void removeQueue(String queueId) {
    try {
      byte[] rowKey = this.queueIdToRowKey(queueId);
      // The rowkey will be null if the queue cannot be found in the Replication Table
      if (rowKey == null) {
        abortable.abort("Could not remove queueId from queueId=" + queueId,
            new ReplicationException("Queue " + "not found queueId=" + queueId));
        return;
      }
      Delete deleteQueue = new Delete(rowKey);
      replicationTable.delete(deleteQueue);
    } catch (IOException e) {
      abortable.abort("Could not remove queueId from queueId=" + queueId, e);
    }
  }

  @Override
  public void addLog(String queueId, String filename) throws ReplicationException {
    try {
      // Check if the queue info (Owner, QueueId) is currently stored in the Replication Table
      if (this.queueIdToRowKey(queueId) == null) {
        // Each queue will have an Owner, QueueId, and a collection of [WAL:offset] key values.
        Put putNewQueue = new Put(Bytes.toBytes(buildServerQueueName(queueId)));
        putNewQueue.addColumn(CF, COL_OWNER, Bytes.toBytes(serverName));
        putNewQueue.addColumn(CF, COL_QUEUE_ID, Bytes.toBytes(queueId));
        putNewQueue.addColumn(CF, Bytes.toBytes(filename), INITIAL_OFFSET);
        replicationTable.put(putNewQueue);
      } else {
        // Otherwise simply add the new log and offset as a new column
        Put putNewLog = new Put(this.queueIdToRowKey(queueId));
        putNewLog.addColumn(CF, Bytes.toBytes(filename), INITIAL_OFFSET);
        replicationTable.put(putNewLog);
      }
    } catch (IOException e) {
      abortable.abort("Could not add queue queueId=" + queueId + " filename=" + filename, e);
    }
  }

  @Override
  public void removeLog(String queueId, String filename) {
    try {
      byte[] rowKey = this.queueIdToRowKey(queueId);
      if (rowKey == null) {
        abortable.abort("Could not remove non-existent log from queueId=" + queueId + ", filename="
            + filename, new ReplicationException());
        return;
      }
      Delete delete = new Delete(rowKey);
      delete.addColumns(CF, Bytes.toBytes(filename));
      replicationTable.delete(delete);
    } catch (IOException e) {
      abortable.abort("Could not remove log from queueId=" + queueId + ", filename=" + filename, e);
    }
  }

  @Override
  public void setLogPosition(String queueId, String filename, long position) {
    try {
      byte[] rowKey = this.queueIdToRowKey(queueId);
      if (rowKey == null) {
        abortable.abort("Could not set position of non-existent log from queueId=" + queueId +
            ", filename=" + filename, new ReplicationException());
        return;
      }
      Put walAndOffset = new Put(rowKey);
      walAndOffset.addColumn(CF, Bytes.toBytes(filename), Bytes.toBytes(position));
      // Check if the log file currently exists as a column. This can be done by checking if an
      // offset exists for the file, any offset must be non-zero
      if (!replicationTable.checkAndPut(rowKey, CF, Bytes.toBytes(filename),
        CompareFilter.CompareOp.GREATER, NEGATIVE_OFFSET, walAndOffset)) {
        abortable.abort("Failed to write replication wal position (filename=" + filename +
            ", position=" + position + ")", new ReplicationException());
      }
    } catch (IOException e) {
      abortable.abort("Failed to write replication wal position (filename=" + filename +
          ", position=" + position + ")", e);
    }
  }

  @Override
  public long getLogPosition(String queueId, String filename) throws ReplicationException {
    try {
      byte[] rowKey = this.queueIdToRowKey(queueId);
      if (rowKey == null) {
        throw new ReplicationException("Could not get position in log for non-existent queue " +
            "queueId=" + queueId + ", filename=" + filename);
      }
      Get getOffset = new Get(rowKey);
      getOffset.addColumn(CF, Bytes.toBytes(filename));
      Result result = replicationTable.get(getOffset);
      if (result.isEmpty()) {
        throw new ReplicationException("Could not read empty result while getting log position " +
            "queueId=" + queueId + ", filename=" + filename);
      }
      return Bytes.toLong(result.getValue(CF, Bytes.toBytes(filename)));
    } catch (IOException e) {
      throw new ReplicationException("Could not get position in log for queueId=" + queueId +
          ", filename=" + filename);
    }
  }

  @Override
  public void removeAllQueues() {
    List<String> myQueueIds = getAllQueues();
    for (String queueId : myQueueIds) {
      removeQueue(queueId);
    }
  }

  @Override
  public List<String> getLogsInQueue(String queueId) {
    List<String> logs = new ArrayList<String>();
    try {
      byte[] rowKey = this.queueIdToRowKey(queueId);
      if (rowKey == null) {
        abortable.abort("Could not get logs from non-existent queueId=" + queueId,
            new ReplicationException());
        return null;
      }
      Get getQueue = new Get(rowKey);
      Result queue = replicationTable.get(getQueue);
      if (queue.isEmpty()) {
        return null;
      }
      Map<byte[], byte[]> familyMap = queue.getFamilyMap(CF);
      for (byte[] cQualifier : familyMap.keySet()) {
        if (Arrays.equals(cQualifier, COL_OWNER) || Arrays.equals(cQualifier, COL_QUEUE_ID)) {
          continue;
        }
        logs.add(Bytes.toString(cQualifier));
      }
    } catch (IOException e) {
      return null;
    }
    return logs;
  }

  @Override
  public List<String> getAllQueues() {
    try {
      return this.getQueuesBelongingToServer(serverName);
    } catch (IOException e) {
      abortable.abort("Could not get all replication queues", e);
      return null;
    }
  }

  @Override
  public SortedMap<String, SortedSet<String>> claimQueues(String regionserver) {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public List<String> getListOfReplicators() {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public boolean isThisOurRegionServer(String regionserver) {
    return this.serverName.equals(regionserver);
  }

  @Override
  public void addPeerToHFileRefs(String peerId) throws ReplicationException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public void addHFileRefs(String peerId, List<String> files) throws ReplicationException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) {
    // TODO
    throw new NotImplementedException();
  }

  /**
   * Gets the Replication Table. Builds and blocks until the table is available if the Replication
   * Table does not exist.
   *
   * @return the Replication Table
   * @throws IOException if the Replication Table takes too long to build
   */
  private Table createAndGetReplicationTable() throws IOException {
    if (!replicationTableExists()) {
      createReplicationTable();
    }
    int maxRetries = conf.getInt("replication.queues.createtable.retries.number", 100);
    RetryCounterFactory counterFactory = new RetryCounterFactory(maxRetries, 100);
    RetryCounter retryCounter = counterFactory.create();
    while (!replicationTableExists()) {
      try {
        retryCounter.sleepUntilNextRetry();
        if (!retryCounter.shouldRetry()) {
          throw new IOException("Unable to acquire the Replication Table");
        }
      } catch (InterruptedException e) {
        return null;
      }
    }
    return connection.getTable(REPLICATION_TABLE_NAME);
  }

  /**
   * Checks whether the Replication Table exists yet
   *
   * @return whether the Replication Table exists
   * @throws IOException
   */
  private boolean replicationTableExists() {
    try {
      return admin.tableExists(REPLICATION_TABLE_NAME);
    } catch (IOException e) {
      return false;
    }
  }

  private void createReplicationTable() throws IOException {
    HTableDescriptor replicationTableDescriptor = new HTableDescriptor(REPLICATION_TABLE_NAME);
    replicationTableDescriptor.addFamily(REPLICATION_COL_DESCRIPTOR);
    admin.createTable(replicationTableDescriptor);
  }

  /**
   * Builds the unique identifier for a queue in the Replication table by appending the queueId to
   * the servername
   *
   * @param queueId a String that identifies the queue
   * @return unique identifier for a queue in the Replication table
   */
  private String buildServerQueueName(String queueId) {
    return serverName + "-" + queueId;
  }

  /**
   * Get the QueueIds belonging to the named server from the ReplicationTable
   *
   * @param server name of the server
   * @return a list of the QueueIds belonging to the server
   * @throws IOException
   */
  private List<String> getQueuesBelongingToServer(String server) throws IOException {
    List<String> queues = new ArrayList<String>();
    Scan scan = new Scan();
    SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF, COL_OWNER,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(server));
    scan.setFilter(filterMyQueues);
    scan.addColumn(CF, COL_QUEUE_ID);
    ResultScanner results = replicationTable.getScanner(scan);
    for (Result result : results) {
      queues.add(Bytes.toString(result.getValue(CF, COL_QUEUE_ID)));
    }
    results.close();
    return queues;
  }

  // TODO: We can cache queueId's if ReplicationQueuesHBaseImpl becomes a bottleneck. We currently
  // TODO: perform scan's over all the rows looking for one with a matching QueueId.

  /**
   * Finds the rowkey of the HBase row corresponding to the provided queue
   *
   * @param queueId string representation of the queue id
   * @return the rowkey of the corresponding queue. This returns null if the corresponding queue
   * cannot be found.
   * @throws IOException
   */
  private byte[] queueIdToRowKey(String queueId) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(CF, COL_QUEUE_ID);
    scan.addColumn(CF, COL_OWNER);
    scan.setMaxResultSize(1);
    // Search for the queue that matches this queueId
    SingleColumnValueFilter filterByQueueId = new SingleColumnValueFilter(CF, COL_QUEUE_ID,
        CompareFilter.CompareOp.EQUAL, Bytes.toBytes(queueId));
    // Make sure that we are the owners of the queue. QueueId's may overlap.
    SingleColumnValueFilter filterByOwner = new SingleColumnValueFilter(CF, COL_OWNER,
        CompareFilter.CompareOp.EQUAL, Bytes.toBytes(serverName));
    // We only want the row key
    FirstKeyOnlyFilter filterOutColumns = new FirstKeyOnlyFilter();
    FilterList filterList = new FilterList(filterByQueueId, filterByOwner, filterOutColumns);
    scan.setFilter(filterList);
    ResultScanner results = replicationTable.getScanner(scan);
    Result result = results.next();
    results.close();
    return (result == null) ? null : result.getRow();
  }
}
