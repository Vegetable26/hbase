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
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.zookeeper.KeeperException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

@InterfaceAudience.Private
public class ReplicationQueuesHBaseImpl extends ReplicationStateZKBase implements ReplicationQueues{

    private Admin admin = null;
    private Connection connection = null;
    private Table replicationTable = null;
    private String serverName = null;

    private final byte[] CF = HTableDescriptor.REPLICATION_FAMILY;
    private final byte[] OWNER = HTableDescriptor.REPLICATION_COL_OWNER_BYTES;
    private final byte[] QUEUE_ID = HTableDescriptor.REPLICATION_COL_QUEUE_ID_BYTES;
    private final byte[] INITIAL_OFFSET = Bytes.toBytes(0L);
    private final byte[] NEGATIVE_OFFSET = Bytes.toBytes(-1L);

    public ReplicationQueuesHBaseImpl(ReplicationQueuesArguments args) throws IOException {
        super(args.getZk(), args.getConf(), args.getAbort());
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        replicationTable = createAndGetReplicationTable();

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
                abortable.abort("Could not remove queueId from queueId=" + queueId, new ReplicationException("Queue " +
                  "not found queueId=" + queueId));
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
            // Check if the queue meta information (Owner, QueueId) is currently stored in the Replication Table
            if (this.queueIdToRowKey(queueId) == null) {
                // Each queue will have an Owner, QueueId, and a collection of [WAL:offset] key values.

                System.out.println("Inserting new queue " + queueId);

                Put putNewQueue = new Put(Bytes.toBytes(buildServerQueueName(queueId)));
                putNewQueue.addColumn(CF, OWNER, Bytes.toBytes(serverName));
                putNewQueue.addColumn(CF, QUEUE_ID, Bytes.toBytes(queueId));
                putNewQueue.addColumn(CF, Bytes.toBytes(filename), INITIAL_OFFSET);
                replicationTable.put(putNewQueue);
            } else {

                System.out.println("Found old queue " + queueId);

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
                abortable.abort("Could not remove non-existent log from queueId=" + queueId + ", filename=" + filename,
                  new ReplicationException());
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
                abortable.abort("Could not set position of non-existent log from queueId=" + queueId + ", filename=" +
                    filename,
                  new ReplicationException());
                return;
            }
            Put walAndOffset = new Put(rowKey);
            walAndOffset.addColumn(CF, Bytes.toBytes(filename), Bytes.toBytes(position));
            // Check if the log file currently exists as a column. This can be done by checking if an offset exists
            // for the file, any offset must be non-zero
            if (!replicationTable.checkAndPut(rowKey, CF, Bytes.toBytes(filename), CompareFilter.CompareOp.GREATER,
              NEGATIVE_OFFSET, walAndOffset)) {
                abortable.abort("Failed to write replication wal position (filename=" + filename
                  + ", position=" + position + ")", new ReplicationException());
            }
        } catch (IOException e) {
            abortable.abort("Failed to write replication wal position (filename=" + filename
                + ", position=" + position + ")", e);
        }
    }

    @Override
    public long getLogPosition(String queueId, String filename) throws ReplicationException {
        try {
            byte[] rowKey = this.queueIdToRowKey(queueId);
            if (rowKey == null) {
                throw new ReplicationException("Could not get position in log for non-existent queue queueId="
                  + queueId + ", filename=" + filename);
            }
            Get getOffset = new Get(rowKey);
            getOffset.addColumn(CF, Bytes.toBytes(filename));
            Result result = replicationTable.get(getOffset);
            if (result.isEmpty()) {
                throw new ReplicationException("Could not read empty result while getting log position queueId="
                        + queueId + ", filename=" + filename);
            }
            return Bytes.toLong(result.getValue(CF, Bytes.toBytes(filename)));
        } catch (IOException e) {
            throw new ReplicationException("Could not get position in log for queueId=" + queueId + ", filename="
                    + filename);
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
        try {
            byte[] rowKey = this.queueIdToRowKey(queueId);
            if (rowKey == null) {
                abortable.abort("Could not get logs for queueId=" + queueId,
                  new ReplicationException("Rowkey not found for queueId=" + queueId));
                return null;
            }
            return getLogsInQueue(rowKey);
        } catch (IOException e) {
            abortable.abort("Could not get logs for queueId=" + queueId, e);
            return null;
        }
    }

    private List<String> getLogsInQueue(byte[] rowKey) {
        List<String> logs = new ArrayList<String>();
        try {
            Get getQueue = new Get(rowKey);
            Result queue = replicationTable.get(getQueue);
            if (queue.isEmpty()) {
                return null;
            }
            Map<byte[], byte[]> familyMap = queue.getFamilyMap(CF);
            for(byte[] cQualifier : familyMap.keySet()) {
                if (Arrays.equals(cQualifier, OWNER) || Arrays.equals(cQualifier, QUEUE_ID)) {
                    continue;
                }
                logs.add(Bytes.toString(cQualifier));
            }
        } catch (IOException e) {
            abortable.abort("Could not get logs in queue", e);
            return null;
        }
        return logs;
    }

    @Override
    public List<String> getAllQueues() {
        List<String> queues = new ArrayList<String>();
        try {
            ResultScanner results = this.getQueuesBelongingToServer(serverName);
            for (Result result : results) {
                queues.add(Bytes.toString(result.getValue(CF, QUEUE_ID)));
            }
            results.close();
        } catch (IOException e) {
            abortable.abort("Could not get all replication queues", e);
            return null;
        }
        return queues;
    }

    @Override
    public SortedMap<String, SortedSet<String>> claimQueues(String regionserver) {
        SortedMap<String, SortedSet<String>> queues = new TreeMap<String, SortedSet<String>>();
        if (isThisOurRegionServer(regionserver)) {
            return queues;
        }
        try {
            ResultScanner queuesToClaim = this.getQueuesBelongingToServer(regionserver);
            for (Result queue : queuesToClaim) {
                if (attemptToClaimQueue(queue, regionserver)) {
                    // TODO: Check whether the peer still exists before adding in the log
                    if (checkPeerExists(Bytes.toString(queue.getValue(CF, QUEUE_ID)))) {
                        SortedSet<String> sortedLogs = new TreeSet<String>();
                        List<String> logs = getLogsInQueue(queue.getRow());
                        for (String log : logs) {
                            sortedLogs.add(log);
                        }
                        String newQueueId = buildClaimedQueueId(Bytes.toString(queue.getValue(CF, QUEUE_ID)),
                          regionserver);
                        queues.put(newQueueId, sortedLogs);
                    } else {
                        replicationTable.delete(new Delete(queue.getRow()));
                    }
                }
            }
        } catch (IOException e) {
            abortable.abort("Received an IOException attempting to claimQueues regionserver=" + regionserver,
              new ReplicationException());
        }
        return queues;
    }

    @Override
    public List<String> getListOfReplicators() {

        // scan all of the queues and return a list of all unique OWNER values

        Set<String> peerServers = new TreeSet<String>();
        try {
            Scan scan = new Scan();
            scan.addColumn(CF, OWNER);
            ResultScanner allQueuesInCluster = replicationTable.getScanner(scan);
            for (Result queue : allQueuesInCluster) {
                peerServers.add(Bytes.toString(queue.getValue(CF, OWNER)));
            }
        } catch (IOException e) {
            abortable.abort("Could not get list of replicators", e);
        }
        return new ArrayList<String>(peerServers);

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
     * Gets the Replication Table. Builds and blocks until the table is available if the Replication Table does not
     * exist.
     * @return the Replication Table
     * @throws IOException if the Replication Table takes too long to build
     */
    private Table createAndGetReplicationTable() throws IOException{
        if (!replicationTableExists()) {
            admin.createTable(HTableDescriptor.REPLICATION_TABLEDESC);
        }
        int maxRetries = conf.getInt("hbase.region.replica.replication.hbaseimpl.max_init_retries", 100);
        RetryCounterFactory counterFactory = new RetryCounterFactory(maxRetries, 100);
        RetryCounter retryCounter = counterFactory.create();
        while (!replicationTableExists()) {
            try {
                retryCounter.sleepUntilNextRetry();
                if (!retryCounter.shouldRetry()) {
                    throw new IOException("Unable to acquire the Replication Table");
                }
            } catch (InterruptedException e) {
                // TODO: If we are shutting down we should just return immediately
                return null;
            }
        }
        return connection.getTable(TableName.REPLICATION_TABLE_NAME);
    }

    /**
     * Checks whether the Replication Table exists yet
     * @return whether the Replication Table exists
     * @throws IOException
     */
    private boolean replicationTableExists() {
        try {
            return admin.tableExists(TableName.REPLICATION_TABLE_NAME);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Builds the unique identifier for a queue in the Replication table by appending the queueId to the servername
     * @param queueId a String that identifies the queue
     * @return unique identifier for a queue in the Replication table
     */
    private String buildServerQueueName(String queueId) {
        return serverName + "-" + queueId;
    }

    /**
     * See the naming convention used by ReplicationQueuesZKImpl:
     * We add the name of the recovered RS to the new znode, we can even do that for queues that were recovered 10
     * times giving a znode like number-startcode-number-otherstartcode-number-anotherstartcode-etc
     * @param originalQueueId the queue's original identifier
     * @param originalServer the name of the server that used to own the queue
     * @return the queue's new identifier post-adoption
     */
    private String buildClaimedQueueId(String originalQueueId, String originalServer) {
        return originalQueueId + "-" + originalServer;
    }

    /**
     * Get the Queues belonging to the named server from the ReplicationTable.
     * @param server name of the server
     * @return a scanner over the Queues belonging to the server with fields "Owner" and "QueueId"
     * @throws IOException
     */
    private ResultScanner getQueuesBelongingToServer(String server) throws IOException{
        Scan scan = new Scan();
        SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF, OWNER,
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(server));
        scan.setFilter(filterMyQueues);
        scan.addColumn(CF, OWNER);
        scan.addColumn(CF, QUEUE_ID);
        ResultScanner results = replicationTable.getScanner(scan);
        return results;
    }

    // TODO: We can cache queueId's if ReplicationQueuesHBaseImpl becomes a bottleneck. We currently perform scan's over
    // TODO: all the rows looking for one with a matching QueueId.
    /**
     * Finds the rowkey of the HBase row corresponding to the provided queue
     * @param queueId string representation of the queue id
     * @return the rowkey of the corresponding queue. This returns null if the corresponding queue cannot be found.
     * @throws IOException
     */
    private byte[] queueIdToRowKey(String queueId) throws IOException{
        Scan scan = new Scan();
        scan.addColumn(CF, QUEUE_ID);
        scan.addColumn(CF, OWNER);
        scan.setMaxResultSize(1);
        // Search for the row that matches this queueId
        SingleColumnValueFilter filterByQueueId = new SingleColumnValueFilter(CF, QUEUE_ID,
          CompareFilter.CompareOp.EQUAL, Bytes.toBytes(queueId));
        // Make sure that we are the owners of the queue. QueueId's may overlap
        SingleColumnValueFilter filterByOwner = new SingleColumnValueFilter(CF, OWNER,
          CompareFilter.CompareOp.EQUAL, Bytes.toBytes(serverName));
        // Only return the row key
        FirstKeyOnlyFilter filterOutColumns = new FirstKeyOnlyFilter();
        // Filter by queueId must be inserted first
        FilterList filterList = new FilterList(filterByQueueId, filterByOwner, filterOutColumns);
        scan.setFilter(filterList);
        ResultScanner results = replicationTable.getScanner(scan);
        Result result = results.next();
        results.close();
        return (result == null) ? null : result.getRow();
    }

    private boolean attemptToClaimQueue (Result queue, String originalServer) throws IOException{
        Put putNewQueueOwner = new Put(queue.getRow());
        putNewQueueOwner.addColumn(CF, OWNER, Bytes.toBytes(serverName));

        String newQueueId = buildClaimedQueueId(Bytes.toString(queue.getValue(CF, QUEUE_ID)), originalServer);
        Put putNewQueueId = new Put(queue.getRow());
        putNewQueueId.addColumn(CF, QUEUE_ID, Bytes.toBytes(newQueueId));

        RowMutations claimAndRenameQueue = new RowMutations(queue.getRow());
        claimAndRenameQueue.add(putNewQueueOwner);
        claimAndRenameQueue.add(putNewQueueId);

        // Attempt to claim ownership for this queue by checking if the current OWNER is the original server. If it not
        // then another RS has already claimed it. If it is we set ourselves as the new owner and update the queue's id
        boolean success = replicationTable.checkAndMutate(queue.getRow(), CF, OWNER, CompareFilter.CompareOp.EQUAL,
          Bytes.toBytes(originalServer), claimAndRenameQueue);
        return success;
    }

    /**
     * Checks if the peer pointed to by the peerId still exists
     * @param peerId peer id of another cluster
     * @return if the peer exists
     */
    private boolean checkPeerExists(String peerId) {
        ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(peerId);
        try {
            return peerExists(replicationQueueInfo.getPeerId());
        } catch (KeeperException e) {
            abortable.abort("Failed to find peer", new ReplicationException());
        }
        return false;
    }
}
