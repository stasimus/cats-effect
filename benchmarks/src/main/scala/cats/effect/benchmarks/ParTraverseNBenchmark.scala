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

import cats.effect.{IO, Outcome}
import cats.effect.syntax.all._
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

/**
 * To run the benchmark from within sbt:
 *
 * Jmh / run -i 10 -wi 10 -f 2 -t 1 cats.effect.benchmarks.ParTraverseNBenchmark
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class ParTraverseNBenchmark {

  @Param(Array("4096"))
  var size: Int = _

  @Param(Array("8", "64"))
  var limit: Int = _

  var items: List[Int] = _

  @Setup
  def setup(): Unit =
    items = (0 until size).toList

  @Benchmark
  def parTraverseN(): Long =
    items.parTraverseN(limit)(i => IO(i + 1L)).map(_.sum).unsafeRunSync()

  @Benchmark
  def parTraverseUnbounded(): Long =
    items.parTraverse(i => IO(i + 1L)).map(_.sum).unsafeRunSync()

  @Benchmark
  def coordinators(): Long =
    coordinated(limit)(items)(i => IO(i + 1L)).map(_.sum).unsafeRunSync()

  private def coordinated[A, B](n: Int)(as: List[A])(f: A => IO[B]): IO[List[B]] =
    as.traverse(a => IO.deferred[B].tupleLeft(a)).flatMap { slots =>
      val work = slots.toVector
      IO.ref(0).flatMap { idx =>
        def step: IO[Unit] =
          idx.modify(i => (i + 1, i)).flatMap { i =>
            if (i >= work.size) IO.unit
            else {
              val (a, slot) = work(i)
              IO.uncancelable { poll =>
                f(a).start.flatMap { fib =>
                  poll(fib.join).onCancel(fib.cancel).flatMap {
                    case Outcome.Succeeded(fb) => fb.flatMap(b => slot.complete(b).void)
                    case Outcome.Errored(e) => IO.raiseError(e)
                    case Outcome.Canceled() => poll(IO.canceled) *> IO.never
                  }
                }
              } >> step
            }
          }

        List.fill(n min work.size)(step).parSequence_ *> slots.traverse(_._2.get)
      }
    }
}
