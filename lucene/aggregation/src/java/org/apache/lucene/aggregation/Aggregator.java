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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;

/** TK */
public abstract class Aggregator {

  String scopeField;
  String scopeDelimiter;

  // TODO: add comment about FacetsCollector

  /**
   * TK
   *
   * @param scopeField TK
   * @param scopeDelimiter TK
   */
  public Aggregator(String scopeField, String scopeDelimiter) {
    this.scopeField = scopeField;
    this.scopeDelimiter = scopeDelimiter;
  }

  /**
   * TK
   *
   * @param scopeFieldValue TK
   * @return TK
   */
  public abstract List<String> getAllScopes(String scopeFieldValue);

  /**
   * TK
   *
   * @param facetsCollector TK
   * @param scopesToAccumulatorsMap TK
   * @throws IOException TK
   */
  public void aggregate(
      FacetsCollector facetsCollector, Map<String, Set<Accumulator>> scopesToAccumulatorsMap)
      throws IOException {
    List<FacetsCollector.MatchingDocs> matchingDocsList = facetsCollector.getMatchingDocs();

    for (FacetsCollector.MatchingDocs matchingDocs : matchingDocsList) {

      try (LeafReader leafReader = matchingDocs.context.reader()) {
        DocIdSetIterator docIdSetIterator = matchingDocs.bits.iterator();
        BinaryDocValues scopeDocValues = DocValues.getBinary(leafReader, scopeField);

        Map<String, DoubleValues> perSegmentFieldDoubleValues = new HashMap<>();

        int doc = docIdSetIterator.docID();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
          List<String> allScopesForDocument =
              getAllScopes(scopeDocValues.binaryValue().utf8ToString());
          accumulateDocumentValues(
              doc,
              leafReader.getContext(),
              allScopesForDocument,
              scopesToAccumulatorsMap,
              perSegmentFieldDoubleValues);
          doc = docIdSetIterator.nextDoc();
        }
      }
    }
  }

  private void accumulateDocumentValues(
      int doc,
      LeafReaderContext readerContext,
      List<String> scopes,
      Map<String, Set<Accumulator>> scopesToAccumulatorsMap,
      Map<String, DoubleValues> perSegmentFieldDoubleValues)
      throws IOException {
    for (String scope : scopes) {
      Set<Accumulator> accumulators = scopesToAccumulatorsMap.get(scope);
      for (Accumulator accumulator : accumulators) {
        String fieldName = accumulator.fieldName;
        if (fieldName.equals("docs")) {
          // Handle for CountAccumulator; fieldName doesn't make sense - hence make fieldName
          // "docs".
          // TODO: can this be done better?
          accumulator.increment();
        } else {
          if (perSegmentFieldDoubleValues.containsKey(fieldName)) {
            perSegmentFieldDoubleValues.put(
                fieldName,
                DoubleValuesSource.fromDoubleField(fieldName).getValues(readerContext, null));
          }
          DoubleValues fieldDoubleValues = perSegmentFieldDoubleValues.get(fieldName);
          fieldDoubleValues.advanceExact(doc);
          accumulator.evaluate(fieldDoubleValues.doubleValue());
        }
      }
    }
  }
}
