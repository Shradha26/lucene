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
package org.apache.lucene.demo.aggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.apache.lucene.aggregation.Accumulator;
import org.apache.lucene.aggregation.Aggregator;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/** TK */
public class SimpleAggregationExample {
  private final Directory indexDir = new ByteBuffersDirectory();

  /** TK */
  public SimpleAggregationExample() {}

  private void index() throws IOException {
    IndexWriterConfig iwc =
        new IndexWriterConfig(new WhitespaceAnalyzer())
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    IndexWriter indexWriter = new IndexWriter(indexDir, iwc);

    Document doc = new Document();
    doc.add(new BinaryDocValuesField("genre", new BytesRef("horror|comedy")));
    doc.add(new DoubleDocValuesField("duration", 120));
    doc.add(new DoubleDocValuesField("rating", 4.3));
    indexWriter.addDocument(doc);

    doc = new Document();
    doc.add(new BinaryDocValuesField("genre", new BytesRef("comedy")));
    doc.add(new DoubleDocValuesField("duration", 60));
    doc.add(new DoubleDocValuesField("rating", 2.5));
    indexWriter.addDocument(doc);

    doc = new Document();
    doc.add(new BinaryDocValuesField("genre", new BytesRef("comedy|action")));
    doc.add(new DoubleDocValuesField("duration", 135));
    doc.add(new DoubleDocValuesField("rating", 3.9));
    indexWriter.addDocument(doc);

    indexWriter.close();
  }

  private void computeAggregations() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    FacetsCollector fc = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), fc);

    // TODO: create layer that can break requested aggregations into this map
    Map<String, List<Accumulator>> scopesToAccumulators = new HashMap<>();
    scopesToAccumulators.put(
        "comedy",
        List.of(
            AccumlatorFactory.makeAccumulator("max_duration"),
            AccumlatorFactory.makeAccumulator("count_docs")));
    scopesToAccumulators.put(
        "action",
        List.of(
            AccumlatorFactory.makeAccumulator("max_duration"),
            AccumlatorFactory.makeAccumulator("min_rating")));
    scopesToAccumulators.put(
        "horror",
        List.of(
            AccumlatorFactory.makeAccumulator("count_docs"),
            AccumlatorFactory.makeAccumulator("min_rating")));

    Aggregator aggregator = SimpleAggregator("genre", "|");
    aggregator.aggregate(fc, scopesToAccumulators);

    // TODO: create layer that can combine aggregation results
    for (String scope : scopesToAccumulators.keySet()) {
      System.out.println(scope);
      List<Accumulator> accumulators = scopesToAccumulators.get(scope);
      for (Accumulator accumulator : accumulators) {
        System.out.println("\t" + accumulator.getValue());
      }
    }
  }

  /**
   * TK
   *
   * @param args TK
   * @throws IOException TK
   */
  public static void main(String[] args) throws IOException {
    SimpleAggregationExample example = new SimpleAggregationExample();
    example.index();
    example.computeAggregations();
  }
}
