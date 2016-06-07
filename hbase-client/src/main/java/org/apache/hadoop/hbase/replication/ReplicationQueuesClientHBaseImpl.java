package org.apache.hadoop.hbase.replication;

import org.apache.zookeeper.KeeperException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Set;

/**
 * Created by jzh on 6/6/16.
 */
public class ReplicationQueuesClientHBaseImpl implements ReplicationQueuesClient {
  @Override
  public void init() throws ReplicationException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public List<String> getListOfReplicators() throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public List<String> getLogsInQueue(String serverName, String queueId) throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public List<String> getAllQueues(String serverName) throws KeeperException {
    // TODO
    throw new NotImplementedException();
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

  @Override
  public Set<String> getAllWALs() throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }
}
