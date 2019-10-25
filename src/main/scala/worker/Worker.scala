package worker

import java.util.UUID

import akka.actor._
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.PreRestart
import akka.actor.typed.Signal
import akka.actor.typed.scaladsl._
import worker.Master.MasterCommand
import worker.Master.WorkerRequestsWork
import worker.WorkExecutor.DoWork

import scala.concurrent.duration._

/**
  * The worker is actually more of a middle manager, delegating the actual work
  * to the WorkExecutor, supervising it and keeping itself available to interact with the work master.
  */
object Worker {

  sealed trait WorkerCommand
  case object WorkIsReady extends WorkerCommand
  case class Ack(id: String) extends WorkerCommand
  case class SubmitWork(work: Work) extends WorkerCommand
  case class WorkComplete(result: String) extends WorkerCommand

  private case object Register extends WorkerCommand
  private case object WorkTimeout extends WorkerCommand

  def apply(
    masterProxy: typed.ActorRef[Master.MasterCommand]
  ): Behavior[WorkerCommand] = Behaviors.setup[WorkerCommand] { ctx =>
    Behaviors.withTimers { timers =>
      val workerId = UUID.randomUUID().toString
      val registerInterval = ctx.system.settings.config
        .getDuration("distributed-workers.worker-registration-interval")
        .getSeconds
        .seconds

      def createWorkExecutor(): typed.ActorRef[DoWork] = {
        // in addition to starting the actor we also watch it, so that
        // if it stops this worker will also be stopped
        val ref = ctx.spawn(WorkExecutor(), "work-executor")
        ctx.watch(ref)
        ref
      }
      val workExecutor = createWorkExecutor()

      masterProxy ! Master.RegisterWorker(workerId, ctx.self)
      timers.startTimerAtFixedRate("register", Register, registerInterval)

      Behaviors
        .supervise(idle(masterProxy, workerId, workExecutor))
        .onFailure[Exception](typed.SupervisorStrategy.restart)
    }
  }

  private def deregisterOnStop(
    masterProxy: typed.ActorRef[MasterCommand],
    workerId: String
  ): PartialFunction[(typed.scaladsl.ActorContext[WorkerCommand], Signal),
                     Behavior[WorkerCommand]] = {
    case (_, PostStop) =>
      masterProxy ! Master.DeRegisterWorker(workerId)
      Behaviors.same
  }

  private def reportWorkFailedOnRestart(
    masterProxy: typed.ActorRef[MasterCommand],
    workerId: String,
    workId: String
  ): PartialFunction[(typed.scaladsl.ActorContext[WorkerCommand], Signal),
                     Behavior[WorkerCommand]] = {
    case (_, PreRestart) =>
      masterProxy ! Master.WorkFailed(workerId, workId)
      Behaviors.same
  }

  private def idle(
    masterProxy: typed.ActorRef[MasterCommand],
    workerId: String,
    workExecutor: typed.ActorRef[WorkExecutor.DoWork]
  ): Behavior[WorkerCommand] = Behaviors.setup[WorkerCommand] { ctx =>
    // FIXME, exhuastive
    Behaviors.receiveMessage[WorkerCommand] {
      case Register =>
        masterProxy ! Master.RegisterWorker(workerId, ctx.self)
        Behaviors.same
      case WorkIsReady =>
        // this is the only state where we reply to WorkIsReady
        masterProxy ! WorkerRequestsWork(workerId, ctx.self.narrow[SubmitWork])
        Behaviors.same

      case SubmitWork(Work(workId, job: Int)) =>
        ctx.log.info("Got work: {}", job)
        workExecutor ! WorkExecutor.DoWork(job, ctx.self)
        working(workerId, workId, masterProxy, workExecutor)

    } receiveSignal deregisterOnStop(masterProxy, workerId)
  }

  private def working(
    workerId: String,
    workId: String,
    masterProxy: typed.ActorRef[MasterCommand],
    workExecutor: typed.ActorRef[WorkExecutor.DoWork]
  ): Behavior[WorkerCommand] = Behaviors.setup { ctx =>
    Behaviors.receiveMessage[WorkerCommand] {
      // FIXME, exhaustive
      case Worker.WorkComplete(result) =>
        ctx.log.info("Work is complete. Result {}.", result)
        masterProxy ! Master
          .WorkIsDone(workerId, workId, result, ctx.self.narrow[Worker.Ack])
        ctx.setReceiveTimeout(5.seconds, WorkTimeout)
        waitForWorkIsDoneAck(
          result,
          masterProxy,
          workExecutor,
          workerId,
          workId
        )

      case _: SubmitWork =>
        ctx.log.warn(
          "Yikes. Master told me to do work, while I'm already working."
        )
        Behaviors.unhandled

      case Register =>
        masterProxy ! Master.RegisterWorker(workerId, ctx.self)
        Behaviors.same
    } receiveSignal (deregisterOnStop(masterProxy, workerId)
      .orElse(reportWorkFailedOnRestart(masterProxy, workerId, workId)))
  }

  private def waitForWorkIsDoneAck(
    result: String,
    masterProxy: typed.ActorRef[MasterCommand],
    workExecutor: typed.ActorRef[WorkExecutor.DoWork],
    workerId: String,
    workId: String
  ): Behavior[WorkerCommand] =
    Behaviors.setup { ctx =>
      Behaviors.receiveMessage[WorkerCommand] {
        case WorkTimeout =>
          ctx.log.info("No ack from master, resending work result")
          masterProxy ! Master.WorkIsDone(
            workerId,
            workId,
            result,
            ctx.self.narrow[Ack]
          )
          Behaviors.same
        case Ack(id) if id == workId =>
          ctx.log.info("Work acked")
          masterProxy ! WorkerRequestsWork(
            workerId,
            ctx.self.narrow[SubmitWork]
          )
          ctx.cancelReceiveTimeout()
          idle(masterProxy, workerId, workExecutor)

        case Register =>
          masterProxy ! Master.RegisterWorker(workerId, ctx.self)
          Behaviors.same
      } receiveSignal (deregisterOnStop(masterProxy, workerId)
        .orElse(reportWorkFailedOnRestart(masterProxy, workerId, workId)))

    }
}
