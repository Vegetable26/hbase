package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Created by jzh on 6/7/16.
 */
public class ReplicationQueuesClientArguments extends ReplicationQueuesArguments {
  public ReplicationQueuesClientArguments(Configuration conf, Abortable abort) {
    super(conf, abort);
  }
  public ReplicationQueuesClientArguments(Configuration conf, Abortable abort, ZooKeeperWatcher zkw)
  {
    super(conf, abort, zkw);
  }
}
