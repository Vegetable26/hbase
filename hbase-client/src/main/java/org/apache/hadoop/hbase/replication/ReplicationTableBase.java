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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * Abstract class that provides an interface to the Replication Table. Which is currently
 * being used for WAL offset tracking.
 * The basic schema of this table will store each individual queue as a
 * seperate row. The row key will be a unique identifier of the creating server's name and the
 * queueId. Each queue must have the following two columns:
 *  COL_QUEUE_OWNER: tracks which server is currently responsible for tracking the queue
 *  COL_QUEUE_OWNER_HISTORY: a "|" delimited list of the previous server's that have owned this
 *    queue. The most recent previous owner is the leftmost entry.
 * They will also have columns mapping [WAL filename : offset]
 * The most flexible method of interacting with the Replication Table is by calling
 * getOrBlockOnReplicationTable() which will return a new copy of the Replication Table. It is up
 * to the caller to close the returned table.
 */
@InterfaceAudience.Private
public abstract class ReplicationTableBase {

  /** Name of the HBase Table used for tracking replication*/
  public static final TableName REPLICATION_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "replication");

  // Column family and column names for Queues in the Replication Table
  public static final byte[] CF_QUEUE = Bytes.toBytes("q");
  public static final byte[] COL_QUEUE_OWNER = Bytes.toBytes("o");
  public static final byte[] COL_QUEUE_OWNER_HISTORY = Bytes.toBytes("h");

  // Column Descriptor for the Replication Table
  private static final HColumnDescriptor REPLICATION_COL_DESCRIPTOR =
    new HColumnDescriptor(CF_QUEUE).setMaxVersions(1)
      .setInMemory(true)
      .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
      .setBloomFilterType(BloomType.NONE);

  // The value used to delimit the queueId and server name inside of a queue's row key. Currently a
  // hyphen, because it is guaranteed that queueId (which is a cluster id) cannot contain hyphens.
  // See HBASE-11394.
  public static final String ROW_KEY_DELIMITER = "-";

  // The value used to delimit server names in the queue history list
  public static final String QUEUE_HISTORY_DELIMITER = "|";

  /*
  * Make sure that normal HBase Replication Table operations for replication have a high number of
  * retries. This is because the server is aborted if any HBase table operation fails. Each RPC will
  * be attempted 240 times before exiting.
  */
  private static final int DEFAULT_CLIENT_RETRIES = 240;
  private static final int DEFAULT_CLIENT_PAUSE = 5000;
  private static final int DEFAULT_RPC_TIMEOUT = 120000;

  /*
  * Make sure that the HBase Replication Table initialization has the proper timeouts. Because
  * HBase servers can come up a lot sooner than the cluster is ready to create tables and this
  * is a one time operation, we can accept longer pauses than normal.
  */
  private static final int DEFAULT_INIT_RETRIES = 240;
  private static final int DEFAULT_INIT_PAUSE = 60000;
  private static final int DEFAULT_INIT_RPC_TIMEOUT = 120000;

  /*
   * Used for fast fail table operations. Primarily ReplicationQueues.addLog(), which blocks
   * during region opening, but is supposed to fail quickly if Replication is not up yet. Increasing
   * these retry values will slow down cluster initialization
   */
  private static final int DEFAULT_FAST_FAIL_RETRIES = 1;
  private static final int DEFAULT_FAST_FAIL_PAUSE = 0;
  private static final int DEFAULT_FAST_FAIL_TIMEOUT = 60000;

  /*
   * Determine the polling frequency used to check when the Replication Table comes up. With the
   * default options we will poll in intervals of 100 ms forever.
   */
  private static final int DEFAULT_WAIT_TABLE_RETRIES = Integer.MAX_VALUE;
  private static final int DEFAULT_WAIT_TABLE_PAUSE = 100;

  // We only need a single thread to initialize the Replication Table
  private static final int NUM_INITIALIZE_WORKERS = 1;

  protected final Configuration conf;
  protected final Configuration fastFailConf;
  protected final Abortable abortable;
  private final Connection connection;
  private final Connection fastFailConnection;
  private final Executor executor;
  private volatile CountDownLatch replicationTableInitialized;
  private int clientRetries;
  private int clientPause;
  private int rpcTimeout;
  private int operationTimeout;
  private int initRetries;
  private int initPause;
  private int initRpcTimeout;
  private int initOperationTimeout;
  private int fastFailTimeout;
  private int fastFailPause;
  private int fastFailRetries;
  private int fastFailOperationTimeout;


  public ReplicationTableBase(Configuration conf, Abortable abort) throws IOException {
    this.conf = new Configuration(conf);
    this.fastFailConf = new Configuration(conf);
    this.abortable = abort;
    readTimeoutConf();
    decorateTimeoutConf();
    this.connection = ConnectionFactory.createConnection(this.conf);
    this.fastFailConnection = ConnectionFactory.createConnection(this.fastFailConf);
    this.executor = setUpExecutor();
    this.replicationTableInitialized = new CountDownLatch(1);
    createReplicationTableInBackground();
  }

  /**
   * Modify the connection's config so that operations run on the Replication Table have longer and
   * a larger number of retries
   */
  private void readTimeoutConf() {
    clientRetries = conf.getInt("hbase.replication.table.client.retries", DEFAULT_CLIENT_RETRIES);
    clientPause = conf.getInt("hbase.replication.table.client.pause", DEFAULT_CLIENT_PAUSE);
    rpcTimeout = conf.getInt("hbase.replication.table.rpc.timeout", DEFAULT_RPC_TIMEOUT);
    initRetries = conf.getInt("hbase.replication.table.init.retries", DEFAULT_INIT_RETRIES);
    initPause = conf.getInt("hbase.replication.table.init.pause", DEFAULT_INIT_PAUSE);
    initRpcTimeout = conf.getInt("hbase.replication.table.init.timeout", DEFAULT_INIT_RPC_TIMEOUT);
    fastFailTimeout= conf.getInt("hbase.replication.table.fastfail.timeout",
        DEFAULT_FAST_FAIL_TIMEOUT);
    fastFailRetries = conf.getInt("hbase.replication.table.fastfail.retries",
        DEFAULT_FAST_FAIL_RETRIES);
    fastFailPause = conf.getInt("hbase.replication.table.fastfail.pause", DEFAULT_FAST_FAIL_PAUSE);
    fastFailOperationTimeout = getOperationTimeout(fastFailRetries, fastFailPause, fastFailTimeout);
    operationTimeout = getOperationTimeout(clientRetries, clientPause, rpcTimeout);
    initOperationTimeout = getOperationTimeout(initRetries, initPause, initRpcTimeout);
  }

  /**
   * Calculate the operation timeout for a retried RPC request
   * @param retries times we retry a request
   * @param pause pause between failed requests
   * @param rpcTimeout timeout of a RPC request
   * @return the operation timeout for a retried RPC request
   */
  private int getOperationTimeout(int retries, int pause, int rpcTimeout) {
    return retries * (pause + rpcTimeout);
  }

  /**
   * Set up the configuration values for normal Replication Table operations
   */
  private void decorateTimeoutConf() {
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, clientPause);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, clientRetries);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, rpcTimeout);
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, operationTimeout);
    conf.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, operationTimeout);
    fastFailConf.setInt(HConstants.HBASE_CLIENT_PAUSE, fastFailPause);
    fastFailConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, fastFailRetries);
    fastFailConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, fastFailTimeout);
    fastFailConf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, fastFailOperationTimeout);
    fastFailConf.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, fastFailOperationTimeout);
  }

  /**
   * Sets up the thread pool executor used to build the Replication Table in the background
   * @return the configured executor
   */
  private Executor setUpExecutor() {
    ThreadPoolExecutor tempExecutor = new ThreadPoolExecutor(NUM_INITIALIZE_WORKERS,
        NUM_INITIALIZE_WORKERS, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat("ReplicationTableExecutor-%d");
    tfb.setDaemon(true);
    tempExecutor.setThreadFactory(tfb.build());
    return tempExecutor;
  }

  /**
   * Get whether the Replication Table has been successfully initialized yet
   * @return whether the Replication Table is initialized
   */
  public boolean getInitializationStatus() {
    return replicationTableInitialized.getCount() == 0;
  }

  /**
   * Build the row key for the given queueId. This will uniquely identify it from all other queues
   * in the cluster.
   * @param serverName The owner of the queue
   * @param queueId String identifier of the queue
   * @return String representation of the queue's row key
   */
  protected String buildQueueRowKey(String serverName, String queueId) {
    return queueId + ROW_KEY_DELIMITER + serverName;
  }

  /**
   * Parse the original queueId from a row key
   * @param rowKey String representation of a queue's row key
   * @return the original queueId
   */
  protected String getRawQueueIdFromRowKey(String rowKey) {
    return rowKey.split(ROW_KEY_DELIMITER)[0];
  }

  /**
   * Returns a queue's row key given either its raw or reclaimed queueId
   *
   * @param queueId queueId of the queue
   * @return byte representation of the queue's row key
   */
  protected byte[] queueIdToRowKey(String serverName, String queueId) {
    // Cluster id's are guaranteed to have no hyphens, so if the passed in queueId has no hyphen
    // then this is not a reclaimed queue.
    if (!queueId.contains(ROW_KEY_DELIMITER)) {
      return Bytes.toBytes(buildQueueRowKey(serverName, queueId));
      // If the queueId contained some hyphen it was reclaimed. In this case, the queueId is the
      // queue's row key
    } else {
      return Bytes.toBytes(queueId);
    }
  }

  /**
   * Creates a "|" delimited record of the queue's past region server owners.
   *
   * @param originalHistory the queue's original owner history
   * @param oldServer the name of the server that used to own the queue
   * @return the queue's new owner history
   */
  protected String buildClaimedQueueHistory(String originalHistory, String oldServer) {
    return oldServer + QUEUE_HISTORY_DELIMITER + originalHistory;
  }

  /**
   * Blocks until the Replication Table is available
   *
   * @throws InterruptedException
   */
  public void blockUntilReplicationIsAvailable() throws InterruptedException {
    replicationTableInitialized.await();
  }

  /**
   * Get a list of all region servers that have outstanding replication queues. These servers could
   * be alive, dead or from a previous run of the cluster.
   * @return a list of server names
   */
  protected List<String> getListOfReplicators() throws ReplicationException{
    // scan all of the queues and return a list of all unique OWNER values
    Set<String> peerServers = new HashSet<String>();
    ResultScanner allQueuesInCluster = null;
    try (Table replicationTable = getOrBlockOnReplicationTable()){
      Scan scan = new Scan();
      scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER);
      allQueuesInCluster = replicationTable.getScanner(scan);
      for (Result queue : allQueuesInCluster) {
        peerServers.add(Bytes.toString(queue.getValue(CF_QUEUE, COL_QUEUE_OWNER)));
      }
    } catch (IOException e) {
      throw new ReplicationException(e);
    } finally {
      if (allQueuesInCluster != null) {
        allQueuesInCluster.close();
      }
    }
    return new ArrayList<String>(peerServers);
  }

  protected List<String> getAllQueues(String serverName) {
    List<String> allQueues = new ArrayList<String>();
    ResultScanner queueScanner = null;
    try {
      queueScanner = getQueuesBelongingToServer(serverName);
      for (Result queue : queueScanner) {
        String rowKey =  Bytes.toString(queue.getRow());
        // If the queue does not have a Owner History, then we must be its original owner. So we
        // want to return its queueId in raw form
        if (Bytes.toString(queue.getValue(CF_QUEUE, COL_QUEUE_OWNER_HISTORY)).length() == 0) {
          allQueues.add(getRawQueueIdFromRowKey(rowKey));
        } else {
          allQueues.add(rowKey);
        }
      }
      return allQueues;
    } catch (IOException e) {
      String errMsg = "Failed getting list of all replication queues for serverName=" + serverName;
      abortable.abort(errMsg, e);
      return null;
    } finally {
      if (queueScanner != null) {
        queueScanner.close();
      }
    }
  }

  protected List<String> getLogsInQueue(String serverName, String queueId) {
    String rowKey = queueId;
    if (!queueId.contains(ROW_KEY_DELIMITER)) {
      rowKey = buildQueueRowKey(serverName, queueId);
    }
    return getLogsInQueue(Bytes.toBytes(rowKey));
  }

  protected List<String> getLogsInQueue(byte[] rowKey) {
    String errMsg = "Failed getting logs in queue queueId=" + Bytes.toString(rowKey);
    try (Table replicationTable = getOrBlockOnReplicationTable()) {
      Get getQueue = new Get(rowKey);
      Result queue = replicationTable.get(getQueue);
      if (queue == null || queue.isEmpty()) {
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return null;
      }
      return readWALsFromResult(queue);
    } catch (IOException e) {
      abortable.abort(errMsg, e);
      return null;
    }
  }

  /**
   * Read all of the WAL's from a queue into a list
   *
   * @param queue HBase query result containing the queue
   * @return a list of all the WAL filenames
   */
  protected List<String> readWALsFromResult(Result queue) {
    List<String> wals = new ArrayList<>();
    Map<byte[], byte[]> familyMap = queue.getFamilyMap(CF_QUEUE);
    for (byte[] cQualifier : familyMap.keySet()) {
      // Ignore the meta data fields of the queue
      if (Arrays.equals(cQualifier, COL_QUEUE_OWNER) || Arrays.equals(cQualifier,
          COL_QUEUE_OWNER_HISTORY)) {
        continue;
      }
      wals.add(Bytes.toString(cQualifier));
    }
    return wals;
  }

  /**
   * Get the queue id's and meta data (Owner and History) for the queues belonging to the named
   * server
   *
   * @param server name of the server
   * @return a ResultScanner over the QueueIds belonging to the server
   * @throws IOException
   */
  protected ResultScanner getQueuesBelongingToServer(String server) throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF_QUEUE, COL_QUEUE_OWNER,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(server));
    scan.setFilter(filterMyQueues);
    scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER);
    scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER_HISTORY);
    try (Table replicationTable = getOrBlockOnReplicationTable()) {
      ResultScanner results = replicationTable.getScanner(scan);
      return results;
    }
  }

  /**
   * Attempts to acquire the Replication Table. This operation will block until it is assigned by
   * the CreateReplicationWorker thread. It is up to the caller of this method to close the
   * returned Table
   * @return the Replication Table when it is created
   * @throws IOException
   */
  protected Table getOrBlockOnReplicationTable() throws IOException {
    // Sleep until the Replication Table becomes available
    try {
      replicationTableInitialized.await();
    } catch (InterruptedException e) {
      String errMsg = "Unable to acquire the Replication Table due to InterruptedException: " +
          e.getMessage();
      throw new InterruptedIOException(errMsg);
    }
    return getAndSetUpReplicationTable(connection);
  }

  /**
   * Attempts to acquire the Replication Table. This operation will immediately throw an exception
   * if the Replication Table is not up yet
   *
   * @return the Replication Table
   * @throws IOException
   */
  protected Table getOrFailFastReplication() throws IOException {
    if (replicationTableInitialized.getCount() != 0) {
      throw new IOException("getOrFailFastReplication() failed because replication is not " +
          "available yet");
    }
    try {
      replicationTableInitialized.await();
    } catch (InterruptedException e) {
      String errMsg = "Unable to acquire the Replication Table due to InterruptedException: " +
          e.getMessage();
      throw new InterruptedIOException(errMsg);
    }
    return getAndSetUpReplicationTable(fastFailConnection);
  }

  /**
   * Creates a new copy of the Replication Table and sets up the proper Table time outs for it
   *
   * @return the Replication Table
   * @throws IOException
   */
  private Table getAndSetUpReplicationTable(Connection connection) throws IOException {
    Table replicationTable = connection.getTable(REPLICATION_TABLE_NAME);
    setReplicationTableTimeOuts(replicationTable);
    return replicationTable;
  }

  /**
   * Increases the RPC and operations timeouts for the Replication Table
   */
  private Table setReplicationTableTimeOuts(Table replicationTable) {
    replicationTable.setRpcTimeout(rpcTimeout);
    replicationTable.setOperationTimeout(operationTimeout);
    return replicationTable;
  }

  /*
   * Checks whether the Replication Table exists yet
   *
   * @return whether the Replication Table exists
   * @throws IOException
   */
  private boolean replicationTableAvailable() {
    try (Admin tempAdmin = connection.getAdmin()){
      return tempAdmin.tableExists(REPLICATION_TABLE_NAME) &&
          tempAdmin.isTableAvailable(REPLICATION_TABLE_NAME);
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Builds the Replication Table in a background thread. Any method accessing the Replication Table
   * should do so through getOrBlockOnReplicationTable()
   *
   * @return the Replication Table
   * @throws IOException if the Replication Table takes too long to build
   */
  private void createReplicationTableInBackground() throws IOException {
    executor.execute(new CreateReplicationTableWorker());
  }

  /**
   * Attempts to build the Replication Table. Will continue blocking until we have a valid
   * Table for the Replication Table.
   */
  private class CreateReplicationTableWorker implements Runnable {

    private Configuration initConf;
    private Connection initConnection;
    private Admin initAdmin;

    @Override
    public void run() {
      try {
        initConf = buildTableInitConf();
        initConnection = ConnectionFactory.createConnection(initConf);
        initAdmin = initConnection.getAdmin();
        createReplicationTable();
        int maxRetries = conf.getInt("hbase.replication.waittable.retries",
            DEFAULT_WAIT_TABLE_RETRIES);
        int pause = conf.getInt("hbase.replication.waittable.retries.pause",
            DEFAULT_WAIT_TABLE_PAUSE);
        RetryCounterFactory counterFactory = new RetryCounterFactory(maxRetries, pause);
        RetryCounter retryCounter = counterFactory.create();
        while (!replicationTableAvailable()) {
          retryCounter.sleepUntilNextRetry();
          if (!retryCounter.shouldRetry()) {
            throw new IOException("Unable to acquire the Replication Table");
          }
        }
        replicationTableInitialized.countDown();
      } catch (IOException | InterruptedException e) {
        abortable.abort("Failed building Replication Table", e);
      }
    }

    /**
     * Create the replication table with the provided HColumnDescriptor REPLICATION_COL_DESCRIPTOR
     * in TableBasedReplicationQueuesImpl
     *
     * @throws IOException
     */
    private void createReplicationTable() throws IOException {
      HTableDescriptor replicationTableDescriptor = new HTableDescriptor(REPLICATION_TABLE_NAME);
      replicationTableDescriptor.addFamily(REPLICATION_COL_DESCRIPTOR);
      try {
        if (initAdmin.tableExists(REPLICATION_TABLE_NAME)) {
          return;
        }
      } catch (Exception e) {
        // If this tableExists is called to early admin will throw a null pointer exception. In this
        // case proceed to create the Replication Table as we normally would.
      }
      try {
        initAdmin.createTableAsync(replicationTableDescriptor, null);
      } catch (TableExistsException e) {
        // In this case we can just continue as normal
      }
    }

    private Configuration buildTableInitConf() {
      Configuration initConfiguration = new Configuration(conf);
      initConfiguration.setInt(HConstants.HBASE_CLIENT_PAUSE, initPause);
      initConfiguration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, initRetries);
      initConfiguration.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, initRpcTimeout);
      initConfiguration.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, initOperationTimeout);
      initConfiguration.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, initOperationTimeout);
      return initConfiguration;
    }
  }
}
