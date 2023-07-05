package org.apache.lucene.aggregation;

import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;

import java.io.IOException;
import java.util.*;

public abstract class Aggregator {

  String scopeField = "scope";

  abstract List<String> getAllScopes(String scopeFieldValue);

  public Map<String, Set<Accumulator>> aggregate(FacetsCollector facetsCollector, Map<String, Set<Accumulator>> scopesToAccumulatorsMap) throws IOException {
    List<FacetsCollector.MatchingDocs> matchingDocsList = facetsCollector.getMatchingDocs();

    for(FacetsCollector.MatchingDocs matchingDocs : matchingDocsList) {

      try(LeafReader leafReader = matchingDocs.context.reader()) {
        DocIdSetIterator docIdSetIterator = matchingDocs.bits.iterator();
        BinaryDocValues scopeDocValues = DocValues.getBinary(leafReader, scopeField);

        Map<String, DoubleValues> perSegmentFieldDoubleValues = new HashMap<>();

        int doc = docIdSetIterator.docID();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
          List<String> allScopesForDocument =  getAllScopes(scopeDocValues.binaryValue().utf8ToString());
          accumulateDocumentValues(doc, leafReader.getContext(),
              allScopesForDocument, scopesToAccumulatorsMap, perSegmentFieldDoubleValues);
          doc = docIdSetIterator.nextDoc();
        }
      }
    }
    return scopesToAccumulatorsMap;
  }

  private void accumulateDocumentValues(int doc, LeafReaderContext readerContext,
                                        List<String> scopes, Map<String,
                                        Set<Accumulator>> scopesToAccumulatorsMap,
                                        Map<String, DoubleValues> perSegmentFieldDoubleValues) throws IOException {
    for(String scope: scopes) {
      Set<Accumulator> accumulators = scopesToAccumulatorsMap.get(scope);
      for(Accumulator accumulator: accumulators) {
        String fieldName = accumulator.fieldName;
        if(perSegmentFieldDoubleValues.containsKey(fieldName)) {
          perSegmentFieldDoubleValues.put(fieldName, DoubleValuesSource.fromDoubleField(fieldName).getValues(readerContext, null));
        }
        DoubleValues fieldDoubleValues = perSegmentFieldDoubleValues.get(fieldName);
        fieldDoubleValues.advanceExact(doc);
        accumulator.evaluate(fieldDoubleValues.doubleValue());
      }
    }
  }
}
