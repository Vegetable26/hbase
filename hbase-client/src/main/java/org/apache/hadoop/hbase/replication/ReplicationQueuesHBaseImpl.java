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

import java.io.IOException;
import java.util.*;

@InterfaceAudience.Private
public class ReplicationQueuesHBaseImpl implements ReplicationQueues{

    // TODO: Consider moving this name to some config
    public static final String REPLICATION_TABLE_NAME = "replication";

    private Connection connection = null;
    private Admin admin = null;
    private Table replicationTable = null;
    private Abortable abort = null;
    private String serverName = null;
    private final byte[] CF = Bytes.toBytes("cf");
    private Map<String, String> queueIdToRowKey = new HashMap<String, String>();

    public ReplicationQueuesHBaseImpl(Configuration conf, Abortable abort) throws IOException {
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.abort = abort;
        TableName replicationTableName = TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR,
                REPLICATION_TABLE_NAME);
        replicationTable = createAndGetTable(replicationTableName);
    }

    @Override
    public void init(String serverName) throws ReplicationException {
        this.serverName = serverName;
    }

    @Override
    public void removeQueue(String queueId) {
        if (!queueIdToRowKey.containsKey(queueId)) {
            abort.abort("Could remove non-existent queueId queueId=" + queueId, new ReplicationException());
            return;
        }
        String rowKey = queueIdToRowKey.get(queueId);
        Delete deleteQueue = new Delete(Bytes.toBytes(rowKey));
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
                putNewQueue.addColumn(CF, Bytes.toBytes("Owner"), Bytes.toBytes(serverName));
                putNewQueue.addColumn(CF, Bytes.toBytes("QueueId"), Bytes.toBytes(queueId));
                replicationTable.put(putNewQueue);
                queueIdToRowKey.put(queueId, buildServerQueueName(queueId));
            } catch (IOException e) {
                throw new ReplicationException("Could not add queue queueId=" + queueId);
            }
        }
        // TODO: Check if we want to initialize initial position to 0 or null
        this.setLogPosition(queueId, filename, 0);
    }

    @Override
    public void removeLog(String queueId, String filename) {
        if (!queueIdToRowKey.containsKey(queueId)) {
            abort.abort("Could not remove non-existent log from queueId=" + queueId + ", filename=" + filename,
                    new ReplicationException());
            return;
        }
        String rowKey = queueIdToRowKey.get(queueId);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(CF, Bytes.toBytes(filename));
        try {
            replicationTable.delete(delete);
        } catch (IOException e) {
            abort.abort("Could not remove log from queueId=" + queueId + ", filename=" + filename, e);
        }
    }

    @Override
    public void setLogPosition(String queueId, String filename, long position) {
        if (!queueIdToRowKey.containsKey(queueId)) {
            abort.abort("Could not set position of non-existent log from queueId=" + queueId + ", filename=" + filename,
                    new ReplicationException());
            return;
        }
        String rowKey = queueIdToRowKey.get(queueId);
        try {
            Put walAndOffset = new Put(Bytes.toBytes(rowKey));
            walAndOffset.addColumn(CF, Bytes.toBytes(filename), Bytes.toBytes(position));
            replicationTable.put(walAndOffset);
        } catch (IOException e) {
            this.abort.abort("Failed to write replication wal position (filename=" + filename
                + ", position=" + position + ")", e);
        }
    }

    @Override
    public long getLogPosition(String queueId, String filename) throws ReplicationException {
        if (!queueIdToRowKey.containsKey(queueId)) {
            throw new ReplicationException("Could not get position in log for non-existent queue queueId="
                    + queueId + ", filename=" + filename);
        }
        String rowKey = queueIdToRowKey.get(queueId);
        Get getOffset = new Get(Bytes.toBytes(rowKey));
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
        if (!queueIdToRowKey.containsKey(queueId)) {
            abort.abort("Could not get logs from non-existent queueId=" + queueId, new ReplicationException());
            return null;
        }
        String rowKey = queueIdToRowKey.get(queueId);
        List<String> logs = new ArrayList<String>();
        Get getQueue = new Get(Bytes.toBytes(rowKey));
        try {
            Result queue = replicationTable.get(getQueue);
            if (queue.isEmpty()) {
                return null;
            }
            Map<byte[], byte[]> familyMap = queue.getFamilyMap(CF);
            for(byte[] cQualifier : familyMap.keySet()) {
                if (Bytes.toString(cQualifier).equals("Owner") || Bytes.toString(cQualifier).equals("QueueId")) {
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
        return new TreeMap<String, SortedSet<String>>();
    }

    @Override
    public List<String> getListOfReplicators() {
        // TODO
        return new ArrayList<String>();
    }

    @Override
    public boolean isThisOurRegionServer(String regionserver) {
        return this.serverName.equals(regionserver);
    }

    @Override
    public void addPeerToHFileRefs(String peerId) throws ReplicationException {
        // TODO
    }

    @Override
    public void addHFileRefs(String peerId, List<String> files) throws ReplicationException {
        // TODO
    }

    @Override
    public void removeHFileRefs(String peerId, List<String> files) {
        // TODO
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
     * Check if the given table exists and create if it doesn't.
     * @param tableName name of the table to get
     * @return the table
     * @throws IOException
     */
    private Table createAndGetTable(TableName tableName) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        // generate the replication table for the entire cluster if it does not exist
        if (!admin.tableExists(tableName)) {
            tableDescriptor.addFamily(new HColumnDescriptor("cf"));
            admin.createTable(tableDescriptor);
        }
        return connection.getTable(tableName);
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
        SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF, Bytes.toBytes("owner"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(server));
        scan.setFilter(filterMyQueues);
        scan.addColumn(CF, Bytes.toBytes("QueueId"));
        ResultScanner results = replicationTable.getScanner(scan);
        for (Result result : results) {
            queues.add(Bytes.toString(result.getValue(CF, Bytes.toBytes("QueueId"))));
        }
        return queues;
    }
}
