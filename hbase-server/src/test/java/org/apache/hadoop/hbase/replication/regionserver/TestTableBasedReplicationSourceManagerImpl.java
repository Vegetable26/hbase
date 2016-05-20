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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummy;
import org.apache.hadoop.hbase.replication.TableBasedReplicationQueuesClientImpl;
import org.apache.hadoop.hbase.replication.TableBasedReplicationQueuesImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

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
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY,
      HConstants.REPLICATION_ENABLE_DEFAULT);
    // To make the Unit Test's run faster we set a lower pause value for table initialization
    conf.setInt("hbase.replication.table.init.pause", 1000);
    conf.setLong("replication.sleep.before.failover", 2000);
    conf.setInt("replication.source.maxretriesmultiplier", 10);

    // Set the table initialization pause lower to speed up the test
    conf.setInt("hbase.replication.table.init.pause", 100);
    conf.setClass("hbase.region.replica.replication.replicationQueues.class",
      TableBasedReplicationQueuesImpl.class, ReplicationQueues.class);
    conf.setClass("hbase.region.replica.replication.replicationQueuesClient.class",
      TableBasedReplicationQueuesClientImpl.class, ReplicationQueuesClient.class);
    utility = new HBaseTestingUtility(conf);
    utility.startMiniCluster();
    setupZkAndReplication();
  }

}
