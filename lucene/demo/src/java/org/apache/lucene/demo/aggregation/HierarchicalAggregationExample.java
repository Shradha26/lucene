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
public class HierarchicalAggregationExample {

  /** TK */
  public HierarchicalAggregationExample() {}

  private final Directory indexDir = new ByteBuffersDirectory();

  private void index() throws IOException {
    IndexWriterConfig iwc =
        new IndexWriterConfig(new WhitespaceAnalyzer())
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    IndexWriter indexWriter = new IndexWriter(indexDir, iwc);

    Document doc = new Document();
    doc.add(
        new BinaryDocValuesField(
            "category",
            new BytesRef("Digital Books/Author/James Joyce|" + "Digital Books/Genre/Modernist")));
    doc.add(new DoubleDocValuesField("unitsSold", 23));
    doc.add(new DoubleDocValuesField("price", 4.99f));
    indexWriter.addDocument(doc);

    doc = new Document();
    doc.add(
        new BinaryDocValuesField(
            "category",
            new BytesRef(
                "Digital Books/Author/James Joyce|"
                    + "Digital Books/Genre/Short Story Collection")));
    doc.add(new DoubleDocValuesField("unitsSold", 15));
    doc.add(new DoubleDocValuesField("price", 3.99f));
    indexWriter.addDocument(doc);

    doc = new Document();
    doc.add(
        new BinaryDocValuesField(
            "category",
            new BytesRef(
                "Digital Books/Author/J.R.R. Tolkien|"
                    + "Digital Books/Genre/Fantasy|"
                    + "Physical Books/Language/English|"
                    + "Physical Books/Format/Hardback")));
    doc.add(new DoubleDocValuesField("unitsSold", 82));
    doc.add(new DoubleDocValuesField("price", 3.99f));
    indexWriter.addDocument(doc);

    doc = new Document();
    doc.add(
        new BinaryDocValuesField(
            "category",
            new BytesRef(
                "Digital Books/Author/J.R.R. Tolkien|"
                    + "Digital Books/Genre/Fantasy|"
                    + "Physical Books/Language/French|"
                    + "Physical Books/Format/Paperback")));
    doc.add(new DoubleDocValuesField("unitsSold", 7));
    doc.add(new DoubleDocValuesField("price", 6.99f));
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
        "Digital Books", List.of(AccumlatorFactory.makeAccumulator("count_docs")));
    scopesToAccumulators.put(
        "Physical Books", List.of(AccumlatorFactory.makeAccumulator("count_docs")));
    scopesToAccumulators.put(
        "Digital Books/Author/J.R.R. Tolkien",
        List.of(
            AccumlatorFactory.makeAccumulator("count_docs"),
            AccumlatorFactory.makeAccumulator("max_unitsSold")));
    scopesToAccumulators.put(
        "Digital Books/Author/James Joyce",
        List.of(
            AccumlatorFactory.makeAccumulator("count_docs"),
            AccumlatorFactory.makeAccumulator("max_unitsSold")));
    scopesToAccumulators.put(
        "Physical Books/Language/English", List.of(AccumlatorFactory.makeAccumulator("min_price")));
    scopesToAccumulators.put(
        "Physical Books/Language/French", List.of(AccumlatorFactory.makeAccumulator("min_price")));

    Aggregator aggregator = HierarchicalAggregator("genre", "|", "/");
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
