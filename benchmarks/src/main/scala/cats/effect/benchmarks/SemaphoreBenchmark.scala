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
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

/**
 * To do comparative benchmarks between versions:
 *
 * benchmarks/run-benchmark SemaphoreBenchmark
 *
 * This will generate results in `benchmarks/results`.
 *
 * Or to run the benchmark from within sbt:
 *
 * Jmh / run -i 10 -wi 10 -f 2 -t 1 cats.effect.benchmarks.SemaphoreBenchmark
 *
 * Which means "10 iterations", "10 warm-up iterations", "2 forks", "1 thread". Please note that
 * benchmarks should be usually executed at least in 10 iterations (as a rule of thumb), but
 * more is better.
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class SemaphoreBenchmark {

  @Param(Array("10", "100", "1000"))
  var waiters: Int = _

  @Param(Array("100"))
  var iterations: Int = _

  @Benchmark
  def cancelForward(): Unit =
    Semaphore[IO](0)
      .flatMap { s =>
        s.acquire.start.replicateA(waiters).flatMap { fibers =>
          IO.cede.replicateA_(waiters) >> fibers.traverse_(_.cancel)
        }
      }
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def cancelReverse(): Unit =
    Semaphore[IO](0)
      .flatMap { s =>
        s.acquire.start.replicateA(waiters).flatMap { fibers =>
          IO.cede.replicateA_(waiters) >> fibers.reverse.traverse_(_.cancel)
        }
      }
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def cancelAllButLast(): Unit =
    Semaphore[IO](0)
      .flatMap { s =>
        s.acquire.start.replicateA(waiters).flatMap { fibers =>
          IO.cede.replicateA_(waiters) >>
            fibers.init.reverse.traverse_(_.cancel) >>
            s.release >>
            fibers.last.joinWithNever
        }
      }
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def happyPath(): Unit =
    Semaphore[IO](1)
      .flatMap { s => s.permit.use_.replicateA_(waiters) }
      .replicateA_(iterations)
      .unsafeRunSync()

  @Benchmark
  def highContention(): Unit =
    Semaphore[IO](1)
      .flatMap { s => s.permit.use_.parReplicateA_(waiters) }
      .replicateA_(iterations)
      .unsafeRunSync()
}
