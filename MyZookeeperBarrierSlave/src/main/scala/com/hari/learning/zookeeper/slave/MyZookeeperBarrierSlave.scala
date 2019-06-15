package com.hari.learning.zookeeper.slave

import org.apache.zookeeper.{ ZooKeeper, Watcher, WatchedEvent }
import org.apache.zookeeper.Watcher.Event.EventType._
import java.util.concurrent.CountDownLatch

class MyZookeeperBarrierSlave(zkHost: String, zkPort: Int) extends Runnable with Watcher {
  var requiresInterruption: Boolean = false
  var latch: CountDownLatch = new CountDownLatch(1)
  val zkBarrierPathName = "/zk_barrier"
  override def run = {
    while (true) {
      Thread.sleep(1000)
      println("Just printing")
      if (requiresInterruption) {
        latch.await()
      }
    }
  }

  override def process(event: WatchedEvent) = {
    event.getType match {
      case NodeCreated => if (zkBarrierPathName.equals(event.getPath)) {
        requiresInterruption = true
        if (!(latch != null && latch.getCount > 0))
          latch = new CountDownLatch(1)
      }
      case NodeDeleted => if (zkBarrierPathName.equals(event.getPath)) {
        requiresInterruption = false
        latch.countDown()
      }
      case NodeDataChanged     => {}
      case NodeChildrenChanged => {}
      case None                => {}
    }
  }

}

object MyZookeeperBarrierSlave {
  import java.util.concurrent.Executors
  def main(args: Array[String]): Unit = {
    val zkHost = args(0)
    val zkHostPort = args(1).toInt
    val threadPoolExecutor = Executors.newFixedThreadPool(2)
    threadPoolExecutor.submit(new MyZookeeperBarrierSlave(zkHost, zkHostPort))
  }
}