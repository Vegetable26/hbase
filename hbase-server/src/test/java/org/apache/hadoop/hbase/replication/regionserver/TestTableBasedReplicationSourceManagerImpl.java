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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesArguments;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummy;
import org.apache.hadoop.hbase.replication.ReplicationTableBase;
import org.apache.hadoop.hbase.replication.TableBasedReplicationQueuesClientImpl;
import org.apache.hadoop.hbase.replication.TableBasedReplicationQueuesImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Tests the ReplicationSourceManager with TableBasedReplicationQueue's and
 * TableBasedReplicationQueuesClient
 */
@Category({ReplicationTests.class, MediumTests.class})
public class TestTableBasedReplicationSourceManagerImpl extends TestReplicationSourceManager {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = HBaseConfiguration.create();
    conf.set("replication.replicationsource.implementation",
      ReplicationSourceDummy.class.getCanonicalName());
    conf.setLong("replication.sleep.before.failover", 2000);
    conf.setInt("replication.source.maxretriesmultiplier", 10);

    conf.setClass("hbase.region.replica.replication.replicationQueues.class",
      TableBasedReplicationQueuesImpl.class, ReplicationQueues.class);
    conf.setClass("hbase.region.replica.replication.replicationQueuesClient.class",
      TableBasedReplicationQueuesClientImpl.class, ReplicationQueuesClient.class);
    utility = new HBaseTestingUtility(conf);
    utility.startMiniCluster();
    setupZkAndReplication();
  }

  /**
   * Test the prelog roll procedure for when Replication is not up. This simulates the cluster
   * initialization process.
   */
  @Test
  public void TestPrelogRoll() throws Exception {
    ReplicationPeers peers = replication.getReplicationManager().getReplicationPeers();
    peers.registerPeer("peer", new ReplicationPeerConfig().setClusterKey("localhost:2818:/bogus1"));
    peers.peerConnected("peer");
    try {
      // Check that the hardcoded WAL name that we use is valid
      TableBasedReplicationQueuesImpl rq = new TableBasedReplicationQueuesImpl(
          new ReplicationQueuesArguments(conf, zkw, zkw));

      List<WALActionsListener> listeners = new ArrayList<>();
      listeners.add(replication);

      rq.blockUntilReplicationAvailable();
      utility.getAdmin().disableTable(ReplicationTableBase.REPLICATION_TABLE_NAME);
      final WALFactory wals = new WALFactory(utility.getConfiguration(), listeners,
          URLEncoder.encode("regionserver:60020", "UTF8"));
      try {
        WAL wal = wals.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace());
        replication.registerWal(wal);
        fail("RegisteringWal should fail while replication is not available");
      } catch (IOException e) {
      }
      final WAL nonReplicatedWal = wals.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace());
      utility.getAdmin().enableTable(ReplicationTableBase.REPLICATION_TABLE_NAME);
      replication.registerWal(nonReplicatedWal);
      utility.getAdmin().disableTable(ReplicationTableBase.REPLICATION_TABLE_NAME);
      try {
        replication.preLogRoll(null, AbstractFSWALProvider.getCurrentFileName(nonReplicatedWal));
        fail("Prelog roll should have attempted to register the log and thrown an exception");
      } catch (IOException e) {
      }
    } finally {
      utility.getAdmin().enableTable(ReplicationTableBase.REPLICATION_TABLE_NAME);
      peers.peerDisconnected("peer");
      peers.unregisterPeer("peer");
    }
  }
}
