import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class SimpleAggregationTest extends LuceneTestCase {
  private final Directory indexDir = new ByteBuffersDirectory();

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

}
