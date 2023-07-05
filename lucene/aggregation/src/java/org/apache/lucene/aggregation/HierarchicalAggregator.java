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
import java.util.List;
import java.util.Set;

/** TK. */
public class HierarchicalAggregator extends Aggregator {
  String subScopeDelimiter;

  /**
   * TK
   *
   * @param scopeField TK
   * @param scopeDelimiter TK
   * @param subScopeDelimiter TK
   */
  public HierarchicalAggregator(
      String scopeField, String scopeDelimiter, String subScopeDelimiter) {
    super(scopeField, scopeDelimiter);
    this.subScopeDelimiter = subScopeDelimiter;
  }

  @Override
  public List<String> getAllScopes(String scopeFieldValue) {
    Set<String> allScopes = new HashSet<>();
    int scopeStart = 0;
    String currentScope;
    // TODO: convert string to char for the delimiters
    for (int i = 0; i < scopeFieldValue.length(); i++) {
      if ('|' == scopeFieldValue.charAt(i)) {
        currentScope = scopeFieldValue.substring(scopeStart, i);
        for (int j = 0; j < currentScope.length(); j++) {
          if ('/' == currentScope.charAt(j)) {
            allScopes.add(currentScope.substring(0, j));
          }
        }
        allScopes.add(currentScope);
        scopeStart = i + 1;
      }
    }
    currentScope = scopeFieldValue.substring(scopeStart);
    for (int j = 0; j < currentScope.length(); j++) {
      if ('/' == currentScope.charAt(j)) {
        allScopes.add(currentScope.substring(0, j));
      }
    }
    allScopes.add(currentScope);
    return allScopes.stream().toList();
  }
}
