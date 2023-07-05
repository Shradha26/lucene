/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.aggregation;

/** Accumulates. */
public abstract class Accumulator {
  double runningValue;
  String fieldName;

  Accumulator(double seed, String fieldName) {
    runningValue = seed;
    this.fieldName = fieldName;
  }

  /**
   * TK
   *
   * @param value TK
   */
  public abstract void evaluate(double value);

  /** TK */
  public void increment() {
    runningValue++;
  }

  /**
   * TK
   *
   * @return double
   */
  public double getValue() {
    return runningValue;
  }

  /**
   * TK
   *
   * @return TK
   */
  @Override
  public String toString() {
    return getClass().toString().replace("Accumulator", "") + "_" + fieldName + ": " + runningValue;
  }
}
