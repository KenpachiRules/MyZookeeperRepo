package com.hari.learning.zookeeper.locks
import org.apache.zookeeper.{ ZooKeeper, Watcher, WatchedEvent, CreateMode, ZooDefs }
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
  var count = 0
  var child: String = ""
  var prevNode: String = null
  var childPos: Int = 0
  if (lockStat != null)
    zk.delete(zkLockParent, lockStat.getVersion)
  zk.create(zkLockParent, "Parent ZNode".getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  override def run: Unit = {
    while (true) {
      Thread.sleep(1000)
      count += 1
      if (count == 5) {
        //create a znode under the lockParent node and check for if there are any smaller children.
        zk.getChildren(zkLockParent, true).foreach(println)
        child = zk.create(zkLockParent + zkPathSep + "child-", "Child".getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL)
        println(s"Created child path is $child")
        val children = zk.getChildren(zkLockParent, true).map(child => zkLockParent + zkPathSep + child)
        Collections.sort(children)
        childPos = children.indexOf(child)
        println("Position of child node in the list : " + childPos)
        if (childPos == 0) {
          // this is the smallest and hence I am the owner of the lock
          print
        } else {
          // double check if the smallestChild exists
          println("Another child with smaller seq_num")
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
    println(s"size of args passed ${args.length}")
    val zkHost = args(0)
    val zkHostPort = args(1).toInt
    val executors = Executors.newFixedThreadPool(2)
    val consCount = args(2).toInt
    executors.submit(new MyZookeeperLocksCons(zkHost, zkHostPort, consCount))
  }

}