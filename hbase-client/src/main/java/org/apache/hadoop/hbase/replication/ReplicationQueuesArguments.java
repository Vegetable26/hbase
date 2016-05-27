package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Created by jzh on 5/27/16.
 */
public class ReplicationQueuesArguments {

  private ZooKeeperWatcher zk;
  private Configuration conf;
  private Abortable abort;

  public ReplicationQueuesArguments(Configuration conf, Abortable abort) {
    this.conf = conf;
    this.abort = abort;
  }

  public ReplicationQueuesArguments(Configuration conf, Abortable abort, ZooKeeperWatcher zk) {
    this(conf, abort);
    setZk(zk);
  }

  public ZooKeeperWatcher getZk() {
    return zk;
  }

  public void setZk(ZooKeeperWatcher zk) {
    this.zk = zk;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Abortable getAbort() {
    return abort;
  }

  public void setAbort(Abortable abort) {
    this.abort = abort;
  }
}
