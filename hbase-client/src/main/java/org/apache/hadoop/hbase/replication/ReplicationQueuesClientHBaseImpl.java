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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.zookeeper.KeeperException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ReplicationQueuesClientHBaseImpl extends ReplicationTableClient
    implements ReplicationQueuesClient {

  public ReplicationQueuesClientHBaseImpl(ReplicationQueuesClientArguments args) throws IOException {
    super(args.getConf(), args.getAbortable());
  }
  public ReplicationQueuesClientHBaseImpl(Configuration conf,
      Abortable abortable) throws IOException {
    super(conf, abortable);
  }

  @Override
  public void init() throws ReplicationException {
    // No-op
  }

  @Override
  public Set<String> getAllWALs() {
    Scan getAllQueues = new Scan();
    Set<String> allWALs = new HashSet<String>();
    try {
      ResultScanner allQueues= replicationTable.getScanner(getAllQueues);
      for (Result queue : allQueues) {
        List<String> logs = getLogsInQueue(queue.getRow());
        for (String log : logs) {
          allWALs.add(log);
        }
      }
      return allWALs;
    } catch (IOException e) {
      abortable.abort("Cannot get all WAL files", e);
      return null;
    }
  }

  @Override
  public int getHFileRefsNodeChangeVersion() throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public List<String> getAllPeersFromHFileRefsQueue() throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public List<String> getReplicableHFiles(String peerId) throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }
}
