/*
 * Copyright (c) 2017 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.uber.engsec.dp.dataflow.domain

/** An abstract domain with just top and bottom values:
  *
  *       ⊤ (true)
  *       |
  *       ⊥ (false)
  */
object BooleanDomain extends AbstractDomain[Boolean] {
  override val bottom: Boolean = false
  override def leastUpperBound(first: Boolean, second: Boolean): Boolean = first || second
}

/** An abstract domain representing an optional *fixed* value, where bottom is None. This lattice has no top element;
  * only one element value may be stored. */
class OptionDomain[T] extends AbstractDomain[Option[T]] {
  override val bottom: Option[T] = Option.empty
  override def leastUpperBound(first: Option[T], second: Option[T]): Option[T] = {
    if (first.equals(second))
      first
    else
      throw new java.util.NoSuchElementException("OptionDomain.leastUpperBound with different element values")
  }
}

/** The void domain, storing nothing */
object UnitDomain extends AbstractDomain[Unit] {
  override val bottom: Unit = ()
  override def leastUpperBound(a: Unit, b: Unit): Unit = bottom
}