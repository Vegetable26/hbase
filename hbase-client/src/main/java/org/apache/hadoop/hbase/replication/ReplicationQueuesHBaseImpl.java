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
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@InterfaceAudience.Private
public class ReplicationQueuesHBaseImpl implements ReplicationQueues{

    private Connection connection = null;
    private Table replicationTable = null;
    private Abortable abort = null;
    private String serverName = null;

    private final byte[] CF = HConstants.REPLICATION_FAMILY;
    private final byte[] OWNER = HTableDescriptor.REPLICATION_COL_OWNER_BYTES;
    private final byte[] QUEUE_ID = HTableDescriptor.REPLICATION_COL_QUEUE_ID_BYTES;

     /*
      * Each ReplicationSource or queue should only own one entry in this map. So concurrent accesses to
      * this class should be safe.
      */
    private Map<String, byte[]> queueIdToRowKey = new ConcurrentHashMap<String, byte[]>();

    public ReplicationQueuesHBaseImpl(ReplicationQueuesArguments args) throws IOException {
        this(args.getConf(), args.getAbort());
    }

    public ReplicationQueuesHBaseImpl(Configuration conf, Abortable abort) throws IOException {
        this.connection = ConnectionFactory.createConnection(conf);
        this.abort = abort;
        replicationTable = connection.getTable(TableName.REPLICATION_TABLE_NAME);
    }

    @Override
    public void init(String serverName) throws ReplicationException {
        this.serverName = serverName;
    }

    @Override
    public void removeQueue(String queueId) {
        byte[] rowKey = queueIdToRowKey.get(queueId);
        if (rowKey == null) {
            return;
        }
        Delete deleteQueue = new Delete(rowKey);
        try {
            replicationTable.delete(deleteQueue);
            queueIdToRowKey.remove(queueId);
        } catch (IOException e) {
            abort.abort("Could not remove queueId from queueId=" + queueId, e);
        }
    }

    @Override
    public void addLog(String queueId, String filename) throws ReplicationException {
        // Add the queue to the Replication Table if we cannot find it in our local map
        if (!queueIdToRowKey.containsKey(queueId)) {
            try {
                // Each queue will have an Owner, QueueId, and a collection of [WAL:offset] key values.
                Put putNewQueue = new Put(Bytes.toBytes(buildServerQueueName(queueId)));
                putNewQueue.addColumn(CF, OWNER, Bytes.toBytes(serverName));
                putNewQueue.addColumn(CF, QUEUE_ID, Bytes.toBytes(queueId));
                replicationTable.put(putNewQueue);
                queueIdToRowKey.put(queueId, Bytes.toBytes(buildServerQueueName(queueId)));
            } catch (IOException e) {
                throw new ReplicationException("Could not add queue queueId=" + queueId);
            }
        }
        // TODO: Check if we want to initialize initial position to 0 or null
        this.setLogPosition(queueId, filename, 0);
    }

    @Override
    public void removeLog(String queueId, String filename) {
        byte[] rowKey = queueIdToRowKey.get(queueId);
        if (rowKey == null) {
            abort.abort("Could not remove non-existent log from queueId=" + queueId + ", filename=" + filename,
              new ReplicationException());
            return;
        }
        Delete delete = new Delete(rowKey);
        delete.addColumns(CF, Bytes.toBytes(filename));
        try {
            replicationTable.delete(delete);
        } catch (IOException e) {
            abort.abort("Could not remove log from queueId=" + queueId + ", filename=" + filename, e);
        }
    }

    @Override
    public void setLogPosition(String queueId, String filename, long position) {
        byte[] rowKey = queueIdToRowKey.get(queueId);
        if (rowKey == null) {
            abort.abort("Could not set position of non-existent log from queueId=" + queueId + ", filename=" + filename,
              new ReplicationException());
            return;
        }
        try {
            Put walAndOffset = new Put(rowKey);
            walAndOffset.addColumn(CF, Bytes.toBytes(filename), Bytes.toBytes(position));
            replicationTable.put(walAndOffset);
        } catch (IOException e) {
            this.abort.abort("Failed to write replication wal position (filename=" + filename
                + ", position=" + position + ")", e);
        }
    }

    @Override
    public long getLogPosition(String queueId, String filename) throws ReplicationException {
        byte[] rowKey = queueIdToRowKey.get(queueId);
        if (rowKey == null) {
            throw new ReplicationException("Could not get position in log for non-existent queue queueId="
              + queueId + ", filename=" + filename);
        }
        Get getOffset = new Get(rowKey);
        getOffset.addColumn(CF, Bytes.toBytes(filename));
        try {
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
        queueIdToRowKey.clear();
    }

    @Override
    public List<String> getLogsInQueue(String queueId) {
        byte[] rowKey = queueIdToRowKey.get(queueId);
        if (rowKey == null) {
            abort.abort("Could not get logs from non-existent queueId=" + queueId, new ReplicationException());
            return null;
        }
        List<String> logs = new ArrayList<String>();
        Get getQueue = new Get(rowKey);
        try {
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
            return null;
        }
        return logs;
    }

    @Override
    public List<String> getAllQueues() {
        try {
            return this.getQueuesBelongingToServer(serverName);
        } catch (IOException e) {
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
     * Builds the unique identifier for a queue in the Replication table by appending the queueId to the servername
     * @param queueId a String that identifies the queue
     * @return unique identifier for a queue in the Replication table
     */
    private String buildServerQueueName(String queueId) {
        return serverName + "-" + queueId;
    }

    /**
     * Get the QueueIds belonging to the named server from the ReplicationTable
     * @param server name of the server
     * @return a list of the QueueIds belonging to the server
     * @throws IOException
     */
    private List<String> getQueuesBelongingToServer(String server) throws IOException{
        List<String> queues = new ArrayList<String>();
        Scan scan = new Scan();
        SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF, OWNER,
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(server));
        scan.setFilter(filterMyQueues);
        scan.addColumn(CF, QUEUE_ID);
        ResultScanner results = replicationTable.getScanner(scan);
        for (Result result : results) {
            queues.add(Bytes.toString(result.getValue(CF, QUEUE_ID)));
        }
        return queues;
    }
}
