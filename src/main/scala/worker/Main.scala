package worker

import java.io.File
import java.util.concurrent.CountDownLatch

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.cassandra.testkit.CassandraLauncher
import com.typesafe.config.{Config, ConfigFactory}
import akka.actor.typed.scaladsl.adapter._

object Main {

  // note that 2551 and 2552 are expected to be seed nodes though, even if
  // the back-end starts at 2000
  val backEndPortRange = 2000 to 2999

  val frontEndPortRange = 3000 to 3999

  def main(args: Array[String]): Unit = {
    args.headOption match {

      case None =>
        startClusterInSameJvm()

      case Some(portString) if portString.matches("""\d+""") =>
        val port = portString.toInt
        if (backEndPortRange.contains(port)) startBackEnd(port)
        else if (frontEndPortRange.contains(port)) startFrontEnd(port)
        else startWorker(port, args.lift(1).map(_.toInt).getOrElse(1))

      case Some("cassandra") =>
        startCassandraDatabase()
        println("Started Cassandra, press Ctrl + C to kill")
        new CountDownLatch(1).await()

    }
  }

  def startClusterInSameJvm(): Unit = {
    startCassandraDatabase()

    // two backend nodes
    startBackEnd(2551)
    startBackEnd(2552)
    // two front-end nodes
    startFrontEnd(3000)
    startFrontEnd(3001)
    // two worker nodes with two worker actors each
    startWorker(5001, 2)
    startWorker(5002, 2)
  }

  /**
    * Start a node with the role backend on the given port. (This may also
    * start the shared journal, see below for details)
    */
  def startBackEnd(port: Int): Unit = {
    val system = ActorSystem[Nothing](Behaviors.setup[Nothing](ctx => {
      MasterSingleton.singleton(ctx.system)
      Behaviors.empty
    }), "ClusterSystem", config(port, "back-end"))
  }

  /**
    * Start a front end node that will submit work to the backend nodes
    */
  // #front-end
  def startFrontEnd(port: Int): Unit = {
    import akka.actor.typed.scaladsl.adapter._
    ActorSystem[Nothing](Behaviors.setup[Nothing] { ctx =>
      ctx.spawn(FrontEndTyped(), "front-end")
      ctx.spawn(WorkResultConsumer(), "consumer")
      Behaviors.empty
    }, "ClusterSystem", config(port, "front-end"))
  }
  // #front-end

  /**
    * Start a worker node, with n actual workers that will accept and process workloads
    */
  // #worker
  def startWorker(port: Int, workers: Int): Unit = {
    ActorSystem[Nothing](
      Behaviors.setup[Nothing] { ctx =>
        val masterProxy = MasterSingleton.singleton(ctx.system)
        (1 to workers)
          .foreach(n => ctx.spawn(Worker(masterProxy), s"worker-$n"))
        Behaviors.empty[Nothing]
      },
      "ClusterSystem",
      config(port, "worker")
    )

  }
  // #worker

  def config(port: Int, role: String): Config =
    ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles=[$role]
    """).withFallback(ConfigFactory.load())

  /**
    * To make the sample easier to run we kickstart a Cassandra instance to
    * act as the journal. Cassandra is a great choice of backend for Akka Persistence but
    * in a real application a pre-existing Cassandra cluster should be used.
    */
  def startCassandraDatabase(): Unit = {
    val databaseDirectory = new File("target/cassandra-db")
    CassandraLauncher.start(
      databaseDirectory,
      CassandraLauncher.DefaultTestConfigResource,
      clean = false,
      port = 9042
    )

    // shut the cassandra instance down when the JVM stops
    sys.addShutdownHook {
      CassandraLauncher.stop()
    }
  }

}
