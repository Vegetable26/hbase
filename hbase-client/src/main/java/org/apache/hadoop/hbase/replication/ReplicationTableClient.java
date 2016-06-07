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
import org.apache.hadoop.hbase.client.Get;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@InterfaceAudience.Private
abstract class ReplicationTableClient {

  /** Name of the HBase Table used for tracking replication*/
  public static final TableName REPLICATION_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "replication");

  // Column family and column names for the Replication Table
  protected static final byte[] CF = Bytes.toBytes("r");
  protected static final byte[] COL_OWNER = Bytes.toBytes("o");
  protected static final byte[] COL_QUEUE_ID = Bytes.toBytes("q");

  // Column Descriptor for the Replication Table
  private static final HColumnDescriptor REPLICATION_COL_DESCRIPTOR =
    new HColumnDescriptor(CF).setMaxVersions(1)
      .setInMemory(true)
      .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        // TODO: Figure out which bloom filter to use
      .setBloomFilterType(BloomType.NONE)
      .setCacheDataInL1(true);

  /*
   * Make sure that HBase table operations for replication have a high number of retries. This is
  * because the server is aborted if any HBase table operation fails. Each RPC will be attempted
  * 3600 times before exiting. This provides each operation with 2 hours of retries
  * before the server is aborted.
  */
  private static final int CLIENT_RETRIES = 3600;
  private static final int RPC_TIMEOUT = 2000;
  private static final int OPERATION_TIMEOUT = CLIENT_RETRIES * RPC_TIMEOUT;

  protected final Table replicationTable;
  protected Configuration conf;
  protected Abortable abortable;
  private final Admin admin;
  private final Connection connection;

  public ReplicationTableClient(Configuration conf, Abortable abort) throws IOException {

    this.conf = new Configuration(conf);
    this.abortable = abort;
    // Modify the connection's config so that the Replication Table it returns has a much higher
    // number of client retries
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, CLIENT_RETRIES);
    this.connection = ConnectionFactory.createConnection(this.conf);
    this.admin = connection.getAdmin();
    replicationTable = createAndGetReplicationTable();
    replicationTable.setRpcTimeout(RPC_TIMEOUT);
    replicationTable.setOperationTimeout(OPERATION_TIMEOUT);
  }

  public List<String> getListOfReplicators() {
    // scan all of the queues and return a list of all unique OWNER values
    Set<String> peerServers = new HashSet<String>();
    ResultScanner allQueuesInCluster = null;
    try {
      Scan scan = new Scan();
      scan.addColumn(CF, COL_OWNER);
      allQueuesInCluster = replicationTable.getScanner(scan);
      for (Result queue : allQueuesInCluster) {
        peerServers.add(Bytes.toString(queue.getValue(CF, COL_OWNER)));
      }
    } catch (IOException e) {
      abortable.abort("Could not get list of replicators", e);
    } finally {
      if (allQueuesInCluster != null) {
        allQueuesInCluster.close();
      }
    }
    return new ArrayList<String>(peerServers);
  }

  public List<String> getLogsInQueue(String server, String queueId) {
    List<String> logs = new ArrayList<String>();
    try {
      byte[] rowKey = this.queueIdToRowKey(server, queueId);
      if (rowKey == null) {
        String errMsg = "Could not get logs from non-existent queueId=" + queueId;
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return null;
      }
      return getLogsInQueue(rowKey);
    } catch (IOException e) {
      abortable.abort("Could not get logs in queue queueId=" + queueId, e);
      return null;
    }
  }

  public List<String> getAllQueues(String serverName) {
    List<String> allQueues = new ArrayList<String>();
    ResultScanner queueScanner = null;
    try {
      queueScanner = getAllQueuesScanner(serverName);
      for (Result queue : queueScanner) {
        allQueues.add(Bytes.toString(queue.getValue(CF, COL_QUEUE_ID)));
      }
      return allQueues;
    } catch (IOException e) {
      abortable.abort("Could not get all replication queues", e);
      return null;
    } finally {
      if (queueScanner != null) {
        queueScanner.close();
      }
    }
  }

  /**
   * Get the QueueIds belonging to the named server from the ReplicationTable
   *
   * @param server name of the server
   * @return a ResultScanner over the QueueIds belonging to the server
   * @throws IOException
   */
  protected ResultScanner getAllQueuesScanner(String server) throws IOException {
    List<String> queues = new ArrayList<String>();
    Scan scan = new Scan();
    SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF, COL_OWNER,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(server));
    scan.setFilter(filterMyQueues);
    scan.addColumn(CF, COL_OWNER);
    scan.addColumn(CF, COL_QUEUE_ID);
    ResultScanner results = replicationTable.getScanner(scan);
    return results;
  }

  protected List<String> getLogsInQueue(byte[] rowKey) {
    List<String> logs = new ArrayList<String>();
    try {
      Get getQueue = new Get(rowKey);
      Result queue = replicationTable.get(getQueue);
      if (queue.isEmpty()) {
        return null;
      }
      Map<byte[], byte[]> familyMap = queue.getFamilyMap(CF);
      for(byte[] cQualifier : familyMap.keySet()) {
        if (Arrays.equals(cQualifier, COL_OWNER) || Arrays.equals(cQualifier, COL_QUEUE_ID)) {
          continue;
        }
        logs.add(Bytes.toString(cQualifier));
      }
    } catch (IOException e) {
      abortable.abort("Could not get logs from queue", e);
      return null;
    }
    return logs;
  }

  /**
   * Finds the row key of the HBase row corresponding to the provided queue. This has to be done,
   * because the row key is [original server name + "-" + queueId0]. And the original server will
   * make calls to getLog(), getQueue(), etc. with the argument queueId = queueId0.
   * On the original server we can build the row key by concatenating servername + queueId0.
   * Yet if the queue is claimed by another server, future calls to getLog(), getQueue(), etc.
   * will be made with the argument queueId = queueId0 + "-" + pastOwner0 + "-" + pastOwner1 ...
   * so we need a way to look up rows by their modified queueId's.
   *
   * TODO: Consider updating the queueId passed to getLog, getQueue()... inside of ReplicationSource
   * TODO: and ReplicationSourceManager or the parsing of the passed in queueId's so that we don't
   * TODO have to scan the table for row keys for each update. See HBASE-15956.
   *
   * TODO: We can also cache queueId's if ReplicationQueuesHBaseImpl becomes a bottleneck. We
   * TODO: currently perform scan's over all the rows looking for one with a matching QueueId.
   *
   * @param serverId string representation of the server name
   * @param queueId string representation of the queue id
   * @return the rowkey of the corresponding queue. This returns null if the corresponding queue
   * cannot be found.
   * @throws IOException
   */
  protected byte[] queueIdToRowKey(String serverId, String queueId) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(CF, COL_QUEUE_ID);
    scan.addColumn(CF, COL_OWNER);
    scan.setMaxResultSize(1);
    // Search for the queue that matches this queueId
    SingleColumnValueFilter filterByQueueId = new SingleColumnValueFilter(CF, COL_QUEUE_ID,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(queueId));
    // Make sure that we are the owners of the queue. QueueId's may overlap.
    SingleColumnValueFilter filterByOwner = new SingleColumnValueFilter(CF, COL_OWNER,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(serverId));
    // We only want the row key
    FirstKeyOnlyFilter filterOutColumns = new FirstKeyOnlyFilter();
    FilterList filterList = new FilterList(filterByQueueId, filterByOwner, filterOutColumns);
    scan.setFilter(filterList);
    ResultScanner results = null;
    try {
      results = replicationTable.getScanner(scan);
      Result result = results.next();
      results.close();
      return (result == null) ? null : result.getRow();
    } finally {
      if (results != null) {
        results.close();
      }
    }
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

  /**
   * Create the replication table with the provided HColumnDescriptor REPLICATION_COL_DESCRIPTOR
   * in ReplicationQueuesHBaseImpl
   * @throws IOException
   */
  private void createReplicationTable() throws IOException {
    HTableDescriptor replicationTableDescriptor = new HTableDescriptor(REPLICATION_TABLE_NAME);
    replicationTableDescriptor.addFamily(REPLICATION_COL_DESCRIPTOR);
    admin.createTable(replicationTableDescriptor);
  }
}
