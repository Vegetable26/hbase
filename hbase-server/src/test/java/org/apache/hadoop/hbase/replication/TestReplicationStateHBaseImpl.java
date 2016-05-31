/**
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

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationStateHBaseImpl {

    private static final Log LOG = LogFactory.getLog(TestReplicationStateHBaseImpl.class);

    private static Configuration conf;
    private static HBaseTestingUtility utility;
    private static Connection connection;
    private static ReplicationQueues rqH;

    private final String server1 = ServerName.valueOf("hostname1.example.org", 1234, -1L).toString();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
        conf = utility.getConfiguration();
        conf.setClass("hbase.region.replica.replication.ReplicationQueuesType", ReplicationQueuesHBaseImpl.class,
                ReplicationQueues.class);
        connection = ConnectionFactory.createConnection(conf);
    }

    @Test
    public void testReplicationStateHBase () {
        DummyServer ds = new DummyServer(server1);
        try {
            rqH = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, ds));
            rqH.init(server1);
            // Check that the proper System Tables have been generated
            Table replicationTable = connection.getTable(TableName.REPLICATION_TABLE_NAME);
            assertTrue(replicationTable.getName().isSystemTable());

        } catch (IOException e) {
            e.printStackTrace();
            fail("testReplicationStateHBaseConstruction received an IOException");
        } catch (ReplicationException e) {
            e.printStackTrace();
            fail("testReplicationStateHBaseConstruction received an ReplicationException");

        }
        try {
            // Test adding in WAL files
            assertEquals(0, rqH.getAllQueues().size());
            rqH.addLog("Queue1", "WALLogFile1.1");
            assertEquals(1, rqH.getAllQueues().size());
            rqH.addLog("Queue1", "WALLogFile1.2");
            rqH.addLog("Queue1", "WALLogFile1.3");
            rqH.addLog("Queue1", "WALLogFile1.4");
            rqH.addLog("Queue2", "WALLogFile2.1");
            rqH.addLog("Queue3", "WALLogFile3.1");
            assertEquals(3, rqH.getAllQueues().size());
            assertEquals(4, rqH.getLogsInQueue("Queue1").size());
            assertEquals(1, rqH.getLogsInQueue("Queue2").size());
            assertEquals(1, rqH.getLogsInQueue("Queue3").size());
            // TODO: Or should we throw an error
            assertNull(rqH.getLogsInQueue("Queue4"));
        } catch (ReplicationException e) {
            e.printStackTrace();
            fail("testAddLog received a ReplicationException");
        }
        try {
            // Test updating the log positions
            assertEquals(0l, rqH.getLogPosition("Queue1", "WALLogFile1.1"));
            rqH.setLogPosition("Queue1", "WALLogFile1.1", 123l);
            assertEquals(123l, rqH.getLogPosition("Queue1", "WALLogFile1.1"));
            rqH.setLogPosition("Queue1", "WALLogFile1.1", 123456789l);
            assertEquals(123456789l, rqH.getLogPosition("Queue1", "WALLogFile1.1"));
            rqH.setLogPosition("Queue2", "WALLogFile2.1", 242l);
            assertEquals(242l, rqH.getLogPosition("Queue2", "WALLogFile2.1"));
            rqH.setLogPosition("Queue3", "WALLogFile3.1", 243l);
            assertEquals(243l, rqH.getLogPosition("Queue3", "WALLogFile3.1"));

            // Test if writing to non-existent queue results in abort
            assertEquals(0, ds.getAbortCount());
            rqH.setLogPosition("NotHereQueue", "WALLogFile3.1", 243l);
            assertEquals(1, ds.getAbortCount());
            rqH.setLogPosition("NotHereQueue", "NotHereFile", 243l);
            assertEquals(2, ds.getAbortCount());
            rqH.setLogPosition("Queue1", "NotHereFile", 243l);
            assertEquals(3, ds.getAbortCount());

            // Test reading log positions for non-existent queues and WAL's
            try {
                rqH.getLogPosition("Queue1", "NotHereWAL");
                fail("Replication queue should have thrown a ReplicationException for reading from a non-existent WAL");
            } catch (ReplicationException e) {
            }
            try {
                rqH.getLogPosition("NotHereQueue", "NotHereWAL");
                fail("Replication queue should have thrown a ReplicationException for reading from a non-existent queue");
            } catch (ReplicationException e) {
            }
            // Test removing logs
            rqH.removeLog("Queue1", "WALLogFile1.1");
            assertEquals(3, rqH.getLogsInQueue("Queue1").size());
            // Test removing queues
            rqH.removeQueue("Queue2");
            assertNull(rqH.getLogsInQueue("Queue2"));
            assertEquals(2, rqH.getAllQueues().size());
            // Test removing all queues for a Region Server
            rqH.removeAllQueues();
            assertEquals(0, rqH.getAllQueues().size());
            assertNull(rqH.getLogsInQueue("Queue1"));
        } catch (ReplicationException e) {
            e.printStackTrace();
            fail("testAddLog received a ReplicationException");
        }
    }

    // TODO: Perhaps just inherit this from TestReplicationStateBase
    static class DummyServer implements Server {
        private String serverName;
        private boolean isAborted = false;
        private boolean isStopped = false;
        private int abortCount = 0;

        public DummyServer(String serverName) {
            this.serverName = serverName;
        }

        @Override
        public Configuration getConfiguration() {
            return conf;
        }

        @Override
        public ZooKeeperWatcher getZooKeeper() {
            return null;
        }

        @Override
        public CoordinatedStateManager getCoordinatedStateManager() {
            return null;
        }

        @Override
        public ClusterConnection getConnection() {
            return null;
        }

        @Override
        public MetaTableLocator getMetaTableLocator() {
            return null;
        }

        @Override
        public ServerName getServerName() {
            return ServerName.valueOf(this.serverName);
        }

        @Override
        public void abort(String why, Throwable e) {
            abortCount++;
            this.isAborted = true;
        }

        @Override
        public boolean isAborted() {
            return this.isAborted;
        }

        @Override
        public void stop(String why) {
            this.isStopped = true;
        }

        @Override
        public boolean isStopped() {
            return this.isStopped;
        }

        @Override
        public ChoreService getChoreService() {
            return null;
        }

        @Override
        public ClusterConnection getClusterConnection() {
            return null;
        }

        public int getAbortCount() {
            return abortCount;
        }
    }
}
