package org.lucentrix.searchhound.index;

import com.google.common.hash.BloomFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.nio.file.Path;
import java.util.*;

public class PlainLuceneIndex {
    private final Directory directory;
    private final IndexWriter writer;
    private final StandardAnalyzer analyzer;

    public PlainLuceneIndex(String indexPath) throws Exception {
        this.directory = FSDirectory.open(Path.of(indexPath));
        this.analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        this.writer = new IndexWriter(directory, config);
    }

    /**
     * Index a document with join key support
     */
    public void indexDocument(String docId, Map<String, Object> fields, String joinKeyField) {
        try {
            Document doc = new Document();
            doc.add(new StringField("id", docId, Field.Store.YES));

            // Add all fields
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                if (entry.getValue() instanceof String) {
                    doc.add(new TextField(entry.getKey(), (String) entry.getValue(), Field.Store.YES));
                } else if (entry.getValue() instanceof Integer) {
                    doc.add(new IntPoint(entry.getKey(), (Integer) entry.getValue()));
                    doc.add(new StoredField(entry.getKey(), (Integer) entry.getValue()));
                }
            }

            // Store join key separately for quick access
            if (fields.containsKey(joinKeyField)) {
                Object joinKey = fields.get(joinKeyField);
                doc.add(new StringField("_join_key", joinKey.toString(), Field.Store.YES));
                doc.add(new SortedDocValuesField("_join_key_dv", new BytesRef(joinKey.toString())));
            }

            writer.addDocument(doc);
            writer.commit();
        } catch (Exception e) {
            throw new RuntimeException("Indexing failed", e);
        }
    }

    /**
     * Search documents by join key using Bloom filter optimization
     */
    public List<Document> searchByJoinKey(String joinKey, BloomFilter bloomFilter) {
        try {
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            // First check Bloom filter to avoid unnecessary searches
            if (!bloomFilter.mightContain(joinKey)) {
                return Collections.emptyList();
            }

            // Search for documents with matching join key
            Query query = new TermQuery(new Term("_join_key", joinKey));
            TopDocs topDocs = searcher.search(query, 1000);

            List<Document> results = new ArrayList<>();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                results.add(searcher.doc(scoreDoc.doc));
            }

            reader.close();
            return results;
        } catch (Exception e) {
            throw new RuntimeException("Search failed", e);
        }
    }

    /**
     * Extract all unique join keys for Bloom filter construction
     */
    public Set<String> extractJoinKeys(String joinKeyField) {
        try {
            IndexReader reader = DirectoryReader.open(directory);
            Set<String> joinKeys = new HashSet<>();

            // Use DocValues for efficient join key extraction
            BinaryDocValues docValues = MultiDocValues.getBinaryValues(reader, "_join_key_dv");

            for (int i = 0; i < reader.maxDoc(); i++) {
                if (docValues.advanceExact(i)) {
                    joinKeys.add(docValues.binaryValue().utf8ToString());
                }
            }

            reader.close();
            return joinKeys;
        } catch (Exception e) {
            throw new RuntimeException("Join key extraction failed", e);
        }
    }

    public void close() throws Exception {
        writer.close();
        directory.close();
    }
}