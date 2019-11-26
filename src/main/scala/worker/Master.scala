package worker

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import worker.WorkState.WorkAccepted
import worker.WorkState.WorkDomainEvent
import worker.WorkState.WorkTimedOut
import akka.actor.typed.scaladsl.adapter._
import worker.Master.Idle
import worker.Master.MasterCommand
import worker.Master.NotifyWorkers
import worker.Master.RegisterWorker
import worker.Master.RegistrationCommand
import worker.Master.WorkerState
import worker.WorkState.WorkCompleted
import worker.WorkState.WorkStarted
import worker.WorkState.WorkerFailed
import worker.Worker.WorkerCommand

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration, _}


/**
  * The master actor keep tracks of all available workers, and all scheduled and ongoing work items
  */
object Master {
  object Workers {
    // make it work on just the worker commands
    def workerManager(workers: Map[String, WorkerState], master: ActorRef[MasterCommand]): Behavior[RegistrationCommand] = Behaviors.setup {
      ctx =>
        val considerWorkerDeadAfter: FiniteDuration =
          ctx.system.settings.config
            .getDuration("distributed-workers.consider-worker-dead-after")
            .getSeconds
            .seconds

        def newStaleWorkerDeadline(): Deadline = considerWorkerDeadAfter.fromNow

        Behaviors.receiveMessage[RegistrationCommand] {
          case RegisterWorker(workerId, replyTo) =>
            if (workers.contains(workerId)) {
              workerManager(workers + (workerId -> workers(workerId)
                .copy(
                  ref = replyTo,
                  staleWorkerDeadline = newStaleWorkerDeadline()
                )), master)
            } else {
              ctx.log.info("Worker registered: {}", workerId)
              val initialWorkerState = WorkerState(
                ref = replyTo,
                status = Idle,
                staleWorkerDeadline = newStaleWorkerDeadline()
              )
              workerManager(workers + (workerId -> initialWorkerState), master)
              // FIXME
              //             if (workState.hasWork)
              //               replyTo ! Worker.WorkIsReady
            }

          case DeRegisterWorker(workerId) =>
            workers.get(workerId) match {
              case Some(WorkerState(_, Busy(workId, _), _)) =>
                // there was a workload assigned to the worker when it left
                ctx.log.info("Busy worker de-registered: {}", workerId)
                master ! WorkFailed(workerId, workId)
              case Some(_) =>
                ctx.log.info("Worker de-registered: {}", workerId)
              case _ =>
            }
           workerManager(workers - workerId, master)

          case NotifyWorkers(state) =>
            if (state.hasWork) {
              workers.foreach {
                case (_, WorkerState(ref, Idle, _)) =>
                  ref ! Worker.WorkIsReady
                case _ => // busy
              }
            }
            Behaviors.same

        }
    }

  }

  // FIXME Command
  sealed trait MasterCommand extends CborSerializable
  sealed trait WorkRegistration extends MasterCommand

  sealed trait RegistrationCommand extends MasterCommand

  case class RegisterWorker(workerId: String, replyTo: ActorRef[WorkerCommand]) extends RegistrationCommand
  case class DeRegisterWorker(workerId: String) extends RegistrationCommand
  private case class NotifyWorkers(state: WorkState) extends RegistrationCommand

  case class WorkerRequestsWork(workerId: String, replyTo: ActorRef[Worker.SubmitWork]) extends MasterCommand
  case class WorkIsDone(workerId: String,
                        workId: String,
                        result: Any,
                        replyTo: ActorRef[Worker.Ack])
      extends MasterCommand
  case class WorkFailed(workerId: String, workId: String) extends MasterCommand

  private case object Cleanup extends MasterCommand

  // External commands
  case class SubmitWork(work: Work, replyTo: ActorRef[Master.Ack]) extends MasterCommand



  def apply(workTimeout: FiniteDuration): Behavior[MasterCommand] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { timers =>
        timers.startSingleTimer("cleanup", Cleanup, workTimeout / 2)

        // No typed pub sub yet
        val mediator = DistributedPubSub(ctx.system.toClassic).mediator

        // the set of available workers is not event sourced as it depends on the current set of workers
        var workers = Map[String, WorkerState]()

        val workersChild = ctx.spawn(Workers.workerManager(Map.empty, ctx.self), "worker-manager")


        def changeWorkerToIdle(workerId: String, workId: String): Unit =
          workers.get(workerId) match {
            case Some(workerState @ WorkerState(_, Busy(`workId`, _), _)) ⇒
              val newWorkerState = workerState.copy(
                status = Idle,
                staleWorkerDeadline = newStaleWorkerDeadline()
              )
              workers += (workerId -> newWorkerState)
            case _ ⇒
            // ok, might happen after standby recovery, worker state is not persisted
          }

        EventSourcedBehavior[MasterCommand, WorkDomainEvent, WorkState](
          persistenceId = PersistenceId.ofUniqueId("master"),
          emptyState = WorkState.empty,
          commandHandler = (workState, command) => {
            command match {

              // #persisting
              case work: SubmitWork =>
                // idempotent
                if (workState.isAccepted(work.work.workId)) {
                  work.replyTo ! Master.Ack(work.work.workId)
                  Effect.none
                } else {
                  ctx.log.info("Accepted work: {}", work.work.workId)
                  Effect.persist(WorkAccepted(work.work)).thenRun { _ ⇒
                    // Ack back to original sender
                    work.replyTo ! Master.Ack(work.work.workId)
                    workersChild ! NotifyWorkers(workState)
                  }
                }
              // #persisting

              case Cleanup =>
                var events = ListBuffer.empty[WorkDomainEvent]
                workers.foreach {
                  case (workerId, WorkerState(_, Busy(workId, timeout), _))
                      if timeout.isOverdue() =>
                    ctx.log.info("Work timed out: {}", workId)
                    workers -= workerId
                    events += WorkTimedOut(workId)
                  case (workerId, WorkerState(_, Idle, lastHeardFrom))
                      if lastHeardFrom.isOverdue() =>
                    ctx.log.info("Too long since heard from worker {}, pruning", workerId)
                    workers -= workerId

                  case _ => // this one is a keeper!
                }

                if (events.nonEmpty) {
                  Effect
                    .persist(events.toList)
                    .thenRun(() => workersChild ! NotifyWorkers(workState))
                } else {
                  Effect.none
                }

              case rw: RegisterWorker =>
                workersChild ! rw
                workers ! NotifyWorkers(workState)
                Effect.none
              // #graceful-remove
              case dr: DeRegisterWorker =>
                workersChild ! dr
                Effect.none
              // #graceful-remove
              case WorkerRequestsWork(workerId, replyTo) =>
                ctx.log.info("Worker {} requesting work", workerId)

                if (workState.hasWork) {
                  workers.get(workerId) match {
                    case Some(workerState @ WorkerState(_, Idle, _)) =>
                      val work = workState.nextWork
                      Effect.persist[WorkDomainEvent, WorkState](WorkStarted(work.workId))
                        .thenRun { _ =>
                            ctx.log.info("Giving worker {} some work {}", workerId, work.workId)
                            val newWorkerState = workerState.copy(
                              status = Busy(work.workId, Deadline.now + workTimeout),
                              staleWorkerDeadline = newStaleWorkerDeadline()
                            )
                            workers += (workerId -> newWorkerState)
                            replyTo ! Worker.SubmitWork(work)
                        }
                    case _ =>
                      Effect.none[WorkDomainEvent, WorkState]
                  }
                } else {
                  Effect.none[WorkDomainEvent, WorkState]
                }
              case WorkIsDone(workerId, workId, result, replyTo) =>
                // idempotent - redelivery from the worker may cause duplicates, so it needs to be
                if (workState.isDone(workId)) {
                  // previous Ack was lost, confirm again that this is done
                  replyTo ! Worker.Ack(workId)
                  Effect.none
                } else if (!workState.isInProgress(workId)) {
                  ctx.log.info(
                    "Work {} not in progress, reported as done by worker {}",
                    workId,
                    workerId
                  )
                  Effect.none
                } else {
                  ctx.log.info("Work {} is done by worker {}", result, workerId)
                  changeWorkerToIdle(workerId, workId)
                  Effect.persist[WorkDomainEvent, WorkState](WorkCompleted(workId, result))
                    .thenRun { _ ⇒
                      mediator ! DistributedPubSubMediator
                        .Publish(ResultsTopic, WorkResult(workId, result))
                      // Ack back to original sender
                      replyTo ! Worker.Ack(workId)
                    }
                }
              case WorkFailed(workerId, workId) =>
                if (workState.isInProgress(workId)) {
                  ctx.log.info("Work {} failed by worker {}", workId, workerId)
                  changeWorkerToIdle(workerId, workId)
                  Effect
                    .persist[WorkDomainEvent, WorkState](WorkerFailed(workId))
                    .thenRun(notifyWorkers)
                } else {
                  Effect.none[WorkDomainEvent, WorkState]
                }
            }
          },
          eventHandler = (workState, event) => workState.updated(event)
        )
      }
    }

  val ResultsTopic = "results"

  case class Ack(workId: String) extends CborSerializable

  sealed trait WorkerStatus
  case object Idle extends WorkerStatus
  case class Busy(workId: String, deadline: Deadline) extends WorkerStatus
  case class WorkerState(ref: ActorRef[WorkerCommand],
                         status: WorkerStatus,
                         staleWorkerDeadline: Deadline)

}
