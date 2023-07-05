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

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.expressions.Expression;

/** Factory to create accumulators. */
public class AccumulatorFactory {

  private AccumulatorFactory() {}

  /**
   * TK
   *
   * @param expression TK
   * @param type TK
   * @return TK
   */
  public static Set<Accumulator> makeAccumulators(Expression expression, String variableDelimiter) {
    Set<Accumulator> accumulators = new HashSet<>();
    for (String variable : expression.variables) {
      Accumulator accumulator = makeAccumulator(variable, variableDelimiter);
//      if (variable.contains(variableDelimiter)) {
//        accumulator = makeAccumulator(variable);
//      } else {
//        // TODO: Init with value from storedAttribute
//        accumulator = makeAccumulator(0, variable);
//      }
      accumulators.add(accumulator);
    }
    return accumulators;
  }

  /**
   * TK
   *
   * @param token TK
   * @return org.apache.lucene.aggregation.Accumulator
   */
  public static Accumulator makeAccumulator(String token, String delim) {
    // format: aggType,field
    String[] tokenParts = token.split(delim);
    return switch (tokenParts[0]) {
      case "sum" -> new SumAccumulator(0, tokenParts[1]);
      case "max" -> new MaxAccumulator(0, tokenParts[1]);
      case "count" -> new CountAccumulator(0, tokenParts[1]);
      default -> throw new IllegalStateException("Unexpected value: " + tokenParts[0]);
    };
  }

  /**
   * TK
   *
   * @param value TK
   * @param fieldName TK
   * @param type TK
   * @return org.apache.lucene.aggregation.Accumulator
   */
//  public static Accumulator makeAccumulator(double value, String fieldName, String type) {
//    return new NoOpAccumulator(value, type, fieldName);
//  }
}
