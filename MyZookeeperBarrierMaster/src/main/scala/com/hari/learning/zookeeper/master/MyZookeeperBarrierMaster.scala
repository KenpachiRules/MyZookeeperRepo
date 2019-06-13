package com.hari.learning.zookeeper.master

import org.apache.zookeeper.{ ZooKeeper, Watcher, WatchedEvent, CreateMode }
import org.apache.zookeeper.Watcher.Event.EventType._
import org.apache.zookeeper.data.{ ACL, Id }
import java.lang.Runnable
import scala.collection.JavaConversions._

class MyZookeeperBarrierMaster(zkHost: String, zkHostPort: Int) extends Runnable {
  val zk = new ZooKeeper(zkHost + ":" + zkHostPort, 20000, null)
  val zkBarrierNodeName = "/zk_barrier"
  override def run: Unit = {
    var count = 10
    while (true) {
      Thread.sleep(2000)
      if (count == 10) {
        count = 0
        val aclList = new ACL(755, new Id("world", "everyone")) :: Nil
        zk.create(zkBarrierNodeName, null, aclList, CreateMode.PERSISTENT)
      }
      count += 1
    }
  }
}

object MyZookeeperBarrierMaster {
  import java.util.concurrent.Executors
  def main(args: Array[String]): Unit = {
    val zkHost: String = args(0)
    val zkHostPort: Int = args(1).toInt
    val executor = Executors.newFixedThreadPool(3)
    val submittedTask = executor.submit(new MyZookeeperBarrierMaster(zkHost, zkHostPort))
    println("MyZookeeperBarrierMaster: Exiting from the main thread. ")
  }
}
