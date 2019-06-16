package com.hari.learning.zookeeper.locks
import org.apache.zookeeper.{ ZooKeeper, Watcher, WatchedEvent,CreateMode }
import org.apache.zookeeper.data.{ACL,Id}
import java.util.concurrent.{ Executors, CountDownLatch }
import java.lang.Runnable
import scala.collection.JavaConversions._

class MyZookeeperLocksCons(zkHost: String, zkPort: Int,consCount:Int) extends Watcher with Runnable {
  val zk: ZooKeeper = new ZooKeeper(zkHost + ":" + zkPort, 1000, this)
  val zkLockParent = "/zk_lock"
  val zkPathSep = "/"
  val zkLockChild = "child"
  val zkLockChildPrefix = "-" 
  var lock: CountDownLatch = null
  val lockStat = zk.exists(zkLockParent, false)
  val aclList = new ACL(755,new Id("world","everyone"))::Nil
  var count  = 0
  var child:String = ""
  if(lockStat == null ) // lock_node does not exist
    zk.create(zkLockParent,null,aclList,CreateMode.PERSISTENT)
  override def run: Unit = {
    while (true) {
      Thread.sleep(1000)
      count +=1
      if(count == 5){
        //create a znode under the lockParent node and check for if there are any smaller children.
        child = zk.create(zkLockParent+zkPathSep+zkLockChild+zkLockChildPrefix
            ,null,aclList,CreateMode.EPHEMERAL_SEQUENTIAL)
        val createdChildPos = child.split("zkLockChildPrefix")(1).toInt
        val children =  zk.getChildren(zkLockParent,true)
        val smallestChild = children.map( str => (str,str.split(zkLockChildPrefix)(1).toInt)).toList(0)
        if(createdChildPos <= smallestChild._2) {
          // this is the smallest 
        }else {
          // double check if the smallestChild exists
          if(zk.exists(smallestChild._1, true) != null) {
            lock = new CountDownLatch(1)
            lock.await
          }
        }
        
      }
    }
  }
  
  override def print():Unit = {
    for(i <- 0 until 5) {
      println(consCount +" is the owner of the lock ")
      Thread.sleep(1000)
    }
    val myChild = zk.exists(child, this)
    zk.delete(, myChild.)
  }
  
  override def process(event: WatchedEvent): Unit = {
    event.getType match {
      case 
      
    }
  }

}

object MyZookeeperLocksCons {

  def main(args: Array[String]): Unit = {
    val zkHost = args(0)
    val zkHostPort = args(1).toInt
    val executors = Executors.newFixedThreadPool(2)
    executors.submit(new MyZookeeperLocksCons(zkHost, zkHostPort))
  }

}