import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import java.util.concurrent.Semaphore
import scala.util.Random

case class Philosopher(id:Integer,
                       hostPort:String,
                       root:String,
                       leftFork:Semaphore,
                       rightFork:Semaphore) extends  Watcher {

  val zookeeper = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val philosopherPath = root + "/philosopher_" + id.toString

  if (zookeeper == null) throw new Exception("zooKeeper is not initialized")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      mutex.notify()
    }
  }

  def eat(): Unit = {
    zookeeper.create(philosopherPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    mutex.synchronized {
      rightFork.acquire()
      println("Philosopher " + id + " takes right fork")
      rightFork.release()

      println("Philosopher " + id + " puts right fork")
      leftFork.acquire()
      println("Philosopher " + id + " takes left fork")
      Thread.sleep((Random.nextInt(9) + 1) * 1000)

      leftFork.release()
      println("Philosopher " + id + " puts left fork")
    }
  }

  def think(): Unit = {
    println("Philosopher " + id + " is thinking")
    zookeeper.delete(philosopherPath, -1)
    Thread.sleep((Random.nextInt(9) + 1) * 1000)
  }

}