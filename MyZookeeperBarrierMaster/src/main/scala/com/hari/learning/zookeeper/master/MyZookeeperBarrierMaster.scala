package com.hari.learning.zookeeper.master

import org.apache.zookeeper.{ ZooKeeper, CreateMode }
import org.apache.zookeeper.data.{ ACL, Id }
import scala.collection.JavaConversions._

object MyZookeeperBarrierMaster {

  def main(args: Array[String]): Unit = {
    val zkHost: String = args(0)
    val zkPort: Int = args(1).toInt
    val zk = new ZooKeeper(zkHost + ":" + zkPort, 5000, null)
    val zkBarrierPath = "/zk_barrier"
    val aclList = new ACL(755, new Id("world", "everyone")) :: Nil
    while (true) {
      zk.create(zkBarrierPath, null, aclList, CreateMode.PERSISTENT)
      Thread.sleep(10000)
      val barrierStat = zk.exists(zkBarrierPath, false)
      val zkNodeVersion = barrierStat.getVersion
      zk.delete(zkBarrierPath, zkNodeVersion)
      Thread.sleep(5000)
    }
  }

}