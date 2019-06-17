package com.hari.learning.zookeeper.locks
import org.apache.zookeeper.{ ZooKeeper, Watcher, WatchedEvent, CreateMode }
import org.apache.zookeeper.Watcher.Event.EventType._
import org.apache.zookeeper.data.{ ACL, Id }
import java.util.concurrent.{ Executors, CountDownLatch }
import java.lang.{ Runnable, Comparable }
import scala.collection.JavaConversions._
import java.util.Collections

class MyZookeeperLocksCons(zkHost: String, zkPort: Int, consCount: Int) extends Watcher with Runnable {
  val zk: ZooKeeper = new ZooKeeper(zkHost + ":" + zkPort, 10000, this)
  val zkLockParent = "/zk_lock"
  val zkPathSep = "/"
  var lock: CountDownLatch = null
  val lockStat = zk.exists(zkLockParent, false)
  val aclList = new ACL(755, new Id("world", "everyone")) :: Nil
  var count = 0
  var child: String = ""
  var prevNode: String = null
  var childPos: Int = 0
  if (lockStat == null) // lock_node does not exist
    zk.create(zkLockParent, null, aclList, CreateMode.PERSISTENT)
  override def run: Unit = {
    while (true) {
      Thread.sleep(1000)
      count += 1
      if (count == 5) {
        //create a znode under the lockParent node and check for if there are any smaller children.
        child = zk.create(zkLockParent + zkPathSep, null, aclList, CreateMode.EPHEMERAL_SEQUENTIAL)
        val children = zk.getChildren(zkLockParent, true).asInstanceOf[scala.collection.immutable.List[String]]
        Collections.sort(children)
        childPos = children.indexOf(child)
        if (childPos == 0) {
          // this is the smallest and hence I am the owner of the lock
          print
        } else {
          // double check if the smallestChild exists
          if (zk.exists(children(childPos - 1), true) != null) {
            prevNode = children(childPos - 1)
            lock = new CountDownLatch(1)
            lock.await
          } else
            print
        }
        count = 0
      }
    }
  }

  def print: Unit = {
    for (i <- 0 until 5) {
      println(consCount + " is the owner of the lock ")
      Thread.sleep(1000)
    }
    val myChild = zk.exists(child, this)
    zk.delete(child, myChild.getVersion)
  }

  override def process(event: WatchedEvent): Unit = {
    event.getType match {
      case NodeCreated => {
        // not yet decided.
      }
      case NodeDeleted => {
        if (event.getPath.equals(prevNode)) {
          // release the lock
          lock.countDown()
        }
      }
      case NodeChildrenChanged => {}
      case NodeDataChanged     => {}
      case None                => {}
    }
  }

}

object MyZookeeperLocksCons {

  def main(args: Array[String]): Unit = {
    val zkHost = args(0)
    val zkHostPort = args(1).toInt
    val executors = Executors.newFixedThreadPool(2)
    val consCount = args(2).toInt
    executors.submit(new MyZookeeperLocksCons(zkHost, zkHostPort, consCount))
  }

}