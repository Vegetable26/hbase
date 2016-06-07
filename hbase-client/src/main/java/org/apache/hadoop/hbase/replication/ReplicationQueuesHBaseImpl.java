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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * This class provides an implementation of the ReplicationQueues interface using an HBase table
 * "Replication Table". The basic schema of this table will store each individual queue as a
 * seperate row. The row key will be a unique identifier of the creating server's name and the
 * queueId. Each queue must have the following two columns:
 *  COL_OWNER: tracks which server is currently responsible for tracking the queue
 *  COL_QUEUE_ID: tracks the queue's id as stored in ReplicationSource
 * They will also have columns mapping [WAL filename : offset]
 */

@InterfaceAudience.Private
public class ReplicationQueuesHBaseImpl extends ReplicationTableClient
    implements ReplicationQueues {

  private static final Log LOG = LogFactory.getLog(ReplicationQueuesHBaseImpl.class);

  // Common byte values used in replication offset tracking
  private static final byte[] INITIAL_OFFSET = Bytes.toBytes(0L);

  private String serverName = null;
  private byte[] serverNameBytes = null;

  // TODO: Only use this variable temporarily. Ideally we want to use HBase to store all replication
  // TODO: information
  private ReplicationStateZKBase replicationState;

  public ReplicationQueuesHBaseImpl(ReplicationQueuesArguments args) throws IOException {
    this(args.getConf(), args.getAbortable(), args.getZk());
  }

  public ReplicationQueuesHBaseImpl(Configuration conf, Abortable abort, ZooKeeperWatcher zkw)
      throws IOException {
    super(conf, abort);
    replicationState = new ReplicationStateZKBase(zkw, conf, abort) {};
  }

  @Override
  public void init(String serverName) throws ReplicationException {
    this.serverName = serverName;
    this.serverNameBytes = Bytes.toBytes(serverName);
  }

  @Override
  public void removeQueue(String queueId) {
    try {
      byte[] rowKey = this.queueIdToRowKey(queueId);
      // The rowkey will be null if the queue cannot be found in the Replication Table
      if (rowKey == null) {
        String errMsg = "Could not remove non-existent queue with queueId=" + queueId;
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return;
      }
      Delete deleteQueue = new Delete(rowKey);
      safeQueueUpdate(deleteQueue);
    } catch (IOException e) {
      abortable.abort("Could not remove queue with queueId=" + queueId, e);
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
        safeQueueUpdate(putNewLog);
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
        String errMsg = "Could not remove log from non-existent queueId=" + queueId + ", filename="
          + filename;
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return;
      }
      Delete delete = new Delete(rowKey);
      delete.addColumns(CF, Bytes.toBytes(filename));
      safeQueueUpdate(delete);
    } catch (IOException e) {
      abortable.abort("Could not remove log from queueId=" + queueId + ", filename=" + filename, e);
    }
  }

  @Override
  public void setLogPosition(String queueId, String filename, long position) {
    try {
      byte[] rowKey = this.queueIdToRowKey(queueId);
      if (rowKey == null) {
        String errMsg = "Could not set position of log from non-existent queueId=" + queueId +
          ", filename=" + filename;
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return;
      }
      // Check that the log exists. addLog() must have been called before setLogPosition().
      Get checkLogExists = new Get(rowKey);
      checkLogExists.addColumn(CF, Bytes.toBytes(filename));
      if (!replicationTable.exists(checkLogExists)) {
        String errMsg = "Could not set position of non-existent log from queueId=" + queueId +
          ", filename=" + filename;
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return;
      }
      // Update the log offset if it exists
      Put walAndOffset = new Put(rowKey);
      walAndOffset.addColumn(CF, Bytes.toBytes(filename), Bytes.toBytes(position));
      safeQueueUpdate(walAndOffset);
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
    return getLogsInQueue(serverName, queueId);
  }

  @Override
  public List<String> getAllQueues() {
    return getAllQueues(serverName);
  }

  @Override
  public SortedMap<String, SortedSet<String>> claimQueues(String regionserver) {
    SortedMap<String, SortedSet<String>> queues = new TreeMap<String, SortedSet<String>>();
    if (isThisOurRegionServer(regionserver)) {
      return queues;
    }
    ResultScanner queuesToClaim = null;
    try {
      queuesToClaim = getAllQueuesScanner(regionserver);
      for (Result queue : queuesToClaim) {
        if (attemptToClaimQueue(queue, regionserver)) {
          String oldQueueId = Bytes.toString(queue.getValue(CF, COL_QUEUE_ID));
          ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(oldQueueId);
          if (replicationState.peerExists(replicationQueueInfo.getPeerId())) {
            SortedSet<String> sortedLogs = new TreeSet<String>();
            List<String> logs = getLogsInQueue(queue.getRow());
            for (String log : logs) {
              sortedLogs.add(log);
            }
            String newQueueId = buildClaimedQueueId(Bytes.toString(queue.getValue(CF,
                COL_QUEUE_ID)), regionserver);
            queues.put(newQueueId, sortedLogs);
            LOG.info(serverName + " has claimed queue " + oldQueueId + " from " + regionserver +
                " now named: " + newQueueId);
          } else {
            // Delete orphaned queues
            safeQueueUpdate(new Delete(queue.getRow()));
            LOG.info(serverName + " has deleted abandoned queue " + oldQueueId + " from " +
                regionserver);
          }
        }
      }
    } catch (IOException e) {
      abortable.abort("Received an IOException attempting to claimQueues regionserver=" +
          regionserver, e);
      queues.clear();
    } catch (KeeperException e) {
      abortable.abort("Received an IOException attempting to claimQueues regionserver=" +
          regionserver, e);
      queues.clear();
    } finally {
      if (queuesToClaim != null) {
        queuesToClaim.close();
      }
    }
    return queues;
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
   * See safeQueueUpdate(RowMutations mutate)
   * @param put Row mutation to perform on the queue
   */
  private void safeQueueUpdate(Put put) {
    RowMutations mutations = new RowMutations(put.getRow());
    try {
      mutations.add(put);
    } catch (IOException e){
      abortable.abort("Failed to update Replication Table because of IOException", e);
    }
    safeQueueUpdate(mutations);
  }

  /**
   * See safeQueueUpdate(RowMutations mutate)
   * @param delete Row mutation to perform on the queue
   */
  private void safeQueueUpdate(Delete delete) {
    RowMutations mutations = new RowMutations(delete.getRow());
    try {
      mutations.add(delete);
    } catch (IOException e) {
      abortable.abort("Failed to update Replication Table because of IOException", e);
    }
    safeQueueUpdate(mutations);
  }

  /**
   * Attempt to mutate a given queue in the Replication Table with a checkAndPut on the OWNER column
   * of the queue. Abort the server if this checkAndPut fails: which means we have somehow lost
   * ownership of the column or an IO Exception has occurred during the transaction.
   *
   * @param mutate Mutation to perform on a given queue
   */
  private void safeQueueUpdate(RowMutations mutate) {
    try {
      boolean updateSuccess = replicationTable.checkAndMutate(mutate.getRow(), CF, COL_OWNER,
        CompareFilter.CompareOp.EQUAL, serverNameBytes, mutate);
      if (!updateSuccess) {
        String errMsg = "Failed to update Replication Table because we lost queue ownership";
        abortable.abort(errMsg, new ReplicationException(errMsg));
      }
    } catch (IOException e) {
      abortable.abort("Failed to update Replication Table because of IOException", e);
    }
  }

  private byte[] queueIdToRowKey(String queueId) throws IOException {
    return queueIdToRowKey(serverName, queueId);
  }

  /**
   * Attempt to claim the given queue with a checkAndPut on the OWNER column. We check that the
   * recently killed server is still the OWNER before we claim it.
   * @param queue The queue that we are trying to claim
   * @param originalServer The server that originally owned the queue
   * @return Whether we successfully claimed the queue
   * @throws IOException
   */
  private boolean attemptToClaimQueue (Result queue, String originalServer) throws IOException{
    Put putNewQueueOwner = new Put(queue.getRow());
    putNewQueueOwner.addColumn(CF, COL_OWNER, Bytes.toBytes(serverName));
    String newQueueId = buildClaimedQueueId(Bytes.toString(queue.getValue(CF, COL_QUEUE_ID)),
        originalServer);
    Put putNewQueueId = new Put(queue.getRow());
    putNewQueueId.addColumn(CF, COL_QUEUE_ID, Bytes.toBytes(newQueueId));
    RowMutations claimAndRenameQueue = new RowMutations(queue.getRow());
    claimAndRenameQueue.add(putNewQueueOwner);
    claimAndRenameQueue.add(putNewQueueId);
    // Attempt to claim ownership for this queue by checking if the current OWNER is the original
    // server. If it is not then another RS has already claimed it. If it is we set ourselves as the
    // new owner and update the queue's id.
    boolean success = replicationTable.checkAndMutate(queue.getRow(), CF, COL_OWNER,
        CompareFilter.CompareOp.EQUAL, Bytes.toBytes(originalServer), claimAndRenameQueue);
    return success;
  }

  /**
   * See the naming convention used by ReplicationQueuesZKImpl:
   * We add the name of the recovered RS to the new znode, we can even do that for queues that were
   * recovered 10 times giving a znode like number-startcode-number-otherstartcode-number-etc
   * @param originalQueueId the queue's original identifier
   * @param originalServer the name of the server that used to own the queue
   * @return the queue's new identifier post-adoption
   */
  private String buildClaimedQueueId(String originalQueueId, String originalServer) {
    return originalQueueId + "-" + originalServer;
  }
}
