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
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationStateHBaseImpl {

    private static final Log LOG = LogFactory.getLog(TestReplicationStateHBaseImpl.class);

    private static Configuration conf;
    private static HBaseTestingUtility utility;
    private static ZooKeeperWatcher zkw;
    private static String replicationZNode;

    private static ReplicationQueues rq1;
    private static ReplicationQueues rq2;
    private static ReplicationQueues rq3;

    private static final String server1 = ServerName.valueOf("hostname1.example.org", 1234, -1L).toString();
    private static final String server2 = ServerName.valueOf("hostname2.example.org", 1234, -1L).toString();
    private static final String server3 = ServerName.valueOf("hostname3.example.org", 1234, -1L).toString();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
        conf = utility.getConfiguration();
        conf.setClass("hbase.region.replica.replication.ReplicationQueuesType", ReplicationQueuesHBaseImpl.class,
                ReplicationQueues.class);
        zkw = HBaseTestingUtility.getZooKeeperWatcher(utility);
        String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
        replicationZNode = ZKUtil.joinZNode(zkw.baseZNode, replicationZNodeName);

        try {
            DummyServer ds1 = new DummyServer(server1);
            rq1 = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, ds1));
            rq1.init(server1);
            DummyServer ds2 = new DummyServer(server2);
            rq2 = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, ds2));
            rq2.init(server2);
            DummyServer ds3 = new DummyServer(server3);
            rq3 = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, ds3));
            rq3.init(server3);
        } catch (Exception e) {
            e.printStackTrace();
            fail("testReplicationStateHBaseConstruction received an Exception");
        }

    }

    @Test
    public void checkNamingSchema() throws Exception{
        DummyServer ds = new DummyServer(server1);
        rq1.init(server1);
        assertTrue(rq1.isThisOurRegionServer(server1));
        assertTrue(!rq1.isThisOurRegionServer(server1 + "a"));
        assertTrue(!rq1.isThisOurRegionServer(null));
        rq1.removeAllQueues();
    }

    @Test
    public void TestSingleReplicationQueuesHBaseImpl () {
        try {
            // Test adding in WAL files
            assertEquals(0, rq1.getAllQueues().size());
            rq1.addLog("Queue1", "WALLogFile1.1");
            assertEquals(1, rq1.getAllQueues().size());
            rq1.addLog("Queue1", "WALLogFile1.2");
            rq1.addLog("Queue1", "WALLogFile1.3");
            rq1.addLog("Queue1", "WALLogFile1.4");
            rq1.addLog("Queue2", "WALLogFile2.1");
            rq1.addLog("Queue3", "WALLogFile3.1");
            assertEquals(3, rq1.getAllQueues().size());
            assertEquals(4, rq1.getLogsInQueue("Queue1").size());
            assertEquals(1, rq1.getLogsInQueue("Queue2").size());
            assertEquals(1, rq1.getLogsInQueue("Queue3").size());
            // TODO: Or should we throw an error
            assertNull(rq1.getLogsInQueue("Queue4"));
        } catch (ReplicationException e) {
            e.printStackTrace();
            fail("testAddLog received a ReplicationException");
        }
        try {
            // Test updating the log positions
            assertEquals(0l, rq1.getLogPosition("Queue1", "WALLogFile1.1"));
            rq1.setLogPosition("Queue1", "WALLogFile1.1", 123l);
            assertEquals(123l, rq1.getLogPosition("Queue1", "WALLogFile1.1"));
            rq1.setLogPosition("Queue1", "WALLogFile1.1", 123456789l);
            assertEquals(123456789l, rq1.getLogPosition("Queue1", "WALLogFile1.1"));
            rq1.setLogPosition("Queue2", "WALLogFile2.1", 242l);
            assertEquals(242l, rq1.getLogPosition("Queue2", "WALLogFile2.1"));
            rq1.setLogPosition("Queue3", "WALLogFile3.1", 243l);
            assertEquals(243l, rq1.getLogPosition("Queue3", "WALLogFile3.1"));

            // Test reading log positions for non-existent queues and WAL's
            try {
                rq1.getLogPosition("Queue1", "NotHereWAL");
                fail("Replication queue should have thrown a ReplicationException for reading from a non-existent WAL");
            } catch (ReplicationException e) {
            }
            try {
                rq1.getLogPosition("NotHereQueue", "NotHereWAL");
                fail("Replication queue should have thrown a ReplicationException for reading from a non-existent queue");
            } catch (ReplicationException e) {
            }
            // Test removing logs
            rq1.removeLog("Queue1", "WALLogFile1.1");
            assertEquals(3, rq1.getLogsInQueue("Queue1").size());
            // Test removing queues
            rq1.removeQueue("Queue2");
            assertNull(rq1.getLogsInQueue("Queue2"));
            assertEquals(2, rq1.getAllQueues().size());
            // Test removing all queues for a Region Server
            rq1.removeAllQueues();
            assertEquals(0, rq1.getAllQueues().size());
            assertNull(rq1.getLogsInQueue("Queue1"));
        } catch (ReplicationException e) {
            e.printStackTrace();
            fail("testAddLog received a ReplicationException");
        } finally {
            rq1.removeAllQueues();
            assertEquals(0, rq1.getAllQueues().size());
        }
    }

    @Test
    public void TestMultipleReplicationQueuesHBaseImpl () {
        assertEquals(0, rq1.getAllQueues().size());
        try {
            // Test adding in WAL files
            rq1.addLog("Queue1", "WALLogFile1.1");
            rq1.addLog("Queue1", "WALLogFile1.2");
            rq1.addLog("Queue1", "WALLogFile1.3");
            rq1.addLog("Queue1", "WALLogFile1.4");
            rq1.addLog("Queue2", "WALLogFile2.1");
            rq1.addLog("Queue3", "WALLogFile3.1");
            rq2.addLog("Queue1", "WALLogFile1.1");
            rq2.addLog("Queue1", "WALLogFile1.2");
            rq2.addLog("Queue2", "WALLogFile2.1");
            rq3.addLog("Queue1", "WALLogFile1.1");
            assertEquals(3, rq1.getAllQueues().size());
            assertEquals(2, rq2.getAllQueues().size());
            assertEquals(1, rq3.getAllQueues().size());
            assertEquals(4, rq1.getLogsInQueue("Queue1").size());
            assertEquals(1, rq1.getLogsInQueue("Queue2").size());
            assertEquals(1, rq1.getLogsInQueue("Queue3").size());
            assertEquals(2, rq2.getLogsInQueue("Queue1").size());
            assertEquals(1, rq2.getLogsInQueue("Queue2").size());
            assertEquals(1, rq3.getLogsInQueue("Queue1").size());
        } catch (ReplicationException e) {
            e.printStackTrace();
            fail("testAddLogs received a ReplicationException");
        }
        try {
            rq1.setLogPosition("Queue1", "WALLogFile1.1", 1l);
            rq1.setLogPosition("Queue1", "WALLogFile1.2", 2l);
            rq1.setLogPosition("Queue1", "WALLogFile1.3", 3l);
            rq1.setLogPosition("Queue2", "WALLogFile2.1", 4l);
            rq1.setLogPosition("Queue2", "WALLogFile2.2", 5l);
            rq1.setLogPosition("Queue3", "WALLogFile3.1", 6l);
            rq2.setLogPosition("Queue1", "WALLogFile1.1", 7l);
            rq2.setLogPosition("Queue2", "WALLogFile2.1", 8l);
            rq3.setLogPosition("Queue1", "WALLogFile1.1", 9l);
            assertEquals(1l, rq1.getLogPosition("Queue1", "WALLogFile1.1"));
            assertEquals(2l, rq1.getLogPosition("Queue1", "WALLogFile1.2"));
            assertEquals(4l, rq1.getLogPosition("Queue2", "WALLogFile2.1"));
            assertEquals(6l, rq1.getLogPosition("Queue3", "WALLogFile3.1"));
            assertEquals(7l, rq2.getLogPosition("Queue1", "WALLogFile1.1"));
            assertEquals(8l, rq2.getLogPosition("Queue2", "WALLogFile2.1"));
            assertEquals(9l, rq3.getLogPosition("Queue1", "WALLogFile1.1"));
        } catch (ReplicationException e) {
            e.printStackTrace();
            fail("testAddLogs threw a ReplicationException");
        }
        try {
            Map<String, SortedSet<String>> claimedQueuesFromRq2 = rq1.claimQueues(server2);
            assertEquals(2, claimedQueuesFromRq2.size());
            assertTrue(claimedQueuesFromRq2.containsKey("Queue1-hostname2.example.org,1234,-1"));
            assertTrue(claimedQueuesFromRq2.containsKey("Queue2-hostname2.example.org,1234,-1"));
            assertEquals(2, claimedQueuesFromRq2.get("Queue1-hostname2.example.org,1234,-1").size());
            assertEquals(1, claimedQueuesFromRq2.get("Queue2-hostname2.example.org,1234,-1").size());
            assertEquals(5, rq1.getAllQueues().size());
            // Check that all the logs in the other queue were claimed
            assertEquals(2, rq1.getLogsInQueue("Queue1-hostname2.example.org,1234,-1").size());
            assertEquals(1, rq1.getLogsInQueue("Queue2-hostname2.example.org,1234,-1").size());
            // Check that the offsets of the claimed queues are the same
            assertEquals(7l, rq1.getLogPosition("Queue1-hostname2.example.org,1234,-1", "WALLogFile1.1"));
            assertEquals(8l, rq1.getLogPosition("Queue2-hostname2.example.org,1234,-1", "WALLogFile2.1"));
            // Check that the queues were properly removed from rq2
            assertEquals(0, rq2.getAllQueues().size());

            // TODO: What do we really want to do here
            /*
            assertNull(rq2.getLogsInQueue("Queue1"));
            assertNull(rq2.getLogsInQueue("Queue2"));
            */

            Map<String, SortedSet<String>> claimedQueuesFromRq1 = rq3.claimQueues(server1);
            assertEquals(5, claimedQueuesFromRq1.size());
            assertEquals(6, rq3.getAllQueues().size());

        } catch (ReplicationException e) {
            e.printStackTrace();
            fail("testClaimQueue threw a ReplicationException");
        }


        rq1.removeAllQueues();
        rq2.removeAllQueues();
        rq3.removeAllQueues();
        assertEquals(0, rq1.getAllQueues().size());
        assertEquals(0, rq2.getAllQueues().size());
        assertEquals(0, rq3.getAllQueues().size());

    }

    @After
    public void tearDown() throws KeeperException, IOException {
        ZKUtil.deleteNodeRecursively(zkw, replicationZNode);
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
