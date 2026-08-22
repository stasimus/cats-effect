/*
 * Copyright 2020-2025 Typelevel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cats.effect.benchmarks

import cats.effect.IO
import cats.effect.std.{Dequeue, PQueue, Queue}
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

/**
 * To do comparative benchmarks between versions:
 *
 * benchmarks/run-benchmark QueueCancelationBenchmark
 *
 * This will generate results in `benchmarks/results`.
 *
 * Or to run the benchmark from within sbt:
 *
 * Jmh / run -i 10 -wi 10 -f 2 -t 1 cats.effect.benchmarks.QueueCancelationBenchmark
 *
 * Which means "10 iterations", "10 warm-up iterations", "2 forks", "1 thread". Please note that
 * benchmarks should be usually executed at least in 10 iterations (as a rule of thumb), but
 * more is better.
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class QueueCancelationBenchmark {

  @Param(Array("10", "100", "1000"))
  var waiters: Int = _

  @Param(Array("100"))
  var iterations: Int = _

  private[this] def cancelTakers(take: IO[Any], reverse: Boolean): IO[Unit] =
    take.start.replicateA(waiters).flatMap { fibers =>
      IO.cede.replicateA_(waiters) >>
        (if (reverse) fibers.reverse else fibers).traverse_(_.cancel)
    }

  private[this] def cancelOfferers(offer: IO[Unit], reverse: Boolean): IO[Unit] =
    offer.start.replicateA(waiters).flatMap { fibers =>
      IO.cede.replicateA_(waiters) >>
        (if (reverse) fibers.reverse else fibers).traverse_(_.cancel)
    }

  @Benchmark
  def boundedConcurrentTakeCancelForward(): Unit =
    Queue
      .boundedForConcurrent[IO, Unit](waiters)
      .flatMap(q => cancelTakers(q.take, reverse = false))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def boundedConcurrentTakeCancelReverse(): Unit =
    Queue
      .boundedForConcurrent[IO, Unit](waiters)
      .flatMap(q => cancelTakers(q.take, reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def boundedConcurrentOfferCancelReverse(): Unit =
    Queue
      .boundedForConcurrent[IO, Unit](1)
      .flatMap(q => q.offer(()) >> cancelOfferers(q.offer(()), reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def boundedAsyncTakeCancelReverse(): Unit =
    Queue
      .boundedForAsync[IO, Unit](waiters)
      .flatMap(q => cancelTakers(q.take, reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def synchronousTakeCancelReverse(): Unit =
    Queue
      .synchronous[IO, Unit]
      .flatMap(q => cancelTakers(q.take, reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def synchronousOfferCancelReverse(): Unit =
    Queue
      .synchronous[IO, Unit]
      .flatMap(q => cancelOfferers(q.offer(()), reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def circularBufferTakeCancelReverse(): Unit =
    Queue
      .circularBuffer[IO, Unit](waiters)
      .flatMap(q => cancelTakers(q.take, reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def dequeueTakeCancelReverse(): Unit =
    Dequeue
      .bounded[IO, Unit](waiters)
      .flatMap(q => cancelTakers(q.take, reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def dequeueOfferCancelReverse(): Unit =
    Dequeue
      .bounded[IO, Unit](1)
      .flatMap(q => q.offer(()) >> cancelOfferers(q.offer(()), reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def pqueueTakeCancelReverse(): Unit =
    PQueue
      .bounded[IO, Int](waiters)
      .flatMap(q => cancelTakers(q.take, reverse = true))
      .replicateA_(iterations)
      .unsafeRunSync()
}
