package org.lucentrix.searchhound.index;

import com.google.common.collect.Lists;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;

public class PlainLuceneIndex {
    private final Directory directory;
    private final IndexWriter writer;
    private volatile IndexReader reader;
    private volatile IndexSearcher searcher;
    private final Path indexPath;

    private BloomFilter<String> bloomFilter;
    private final String joinKeyField;

    public PlainLuceneIndex(Path indexPath, String joinKeyField, int capacity) {
        try {
            this.indexPath = indexPath;
            this.joinKeyField = joinKeyField;
            this.directory = FSDirectory.open(indexPath);
            StandardAnalyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            this.writer = new IndexWriter(directory, config);
            this.reader = null;
            this.searcher = null;
            this.bloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(Charset.defaultCharset()),
                    capacity,  // Expected number of elements
                    0.01      // Desired false positive probability
            );
        } catch (Exception ex) {
            throw new RuntimeException("Error creating index "+indexPath, ex);
        }
    }

    public int numDocs() {
        return writer.getDocStats().numDocs;
    }

    public void indexDocuments(Collection<Map<String, Object>> fieldDocs) {
        Set<String> commitJoinKeys = new HashSet<>();
        for (Map<String, Object> fields : fieldDocs) {
            indexDocument(fields);

            Object joinKeyValue = fields.get(joinKeyField);
            if (joinKeyValue != null) {
                bloomFilter.put(String.valueOf(joinKeyValue));
            }
        }
    }

    /**
     * Index a document with join key support
     */
    public void indexDocument(Map<String, Object> fields) {
        try {
            Document doc = new Document();

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
                //  doc.add(new SortedDocValuesField("_join_key_dv", new BytesRef(joinKey.toString())));
                doc.add(new BinaryDocValuesField("_join_key_dv", new BytesRef(joinKey.toString())));
            }

            writer.addDocument(doc);
        } catch (Exception e) {
            throw new RuntimeException("Indexing failed", e);
        }
    }


    public void commit() {
        try {
            writer.commit();

            refreshReader();
        } catch (Exception e) {
            throw new RuntimeException("Commit failed", e);
        }
    }

    private void refreshReader() throws IOException {
        if (reader != null) {
            IndexReader newReader = DirectoryReader.openIfChanged((DirectoryReader) reader);
            if (newReader != null) {
                reader.close();
                reader = newReader;
                searcher = new IndexSearcher(reader);
            }
        }
    }

    private void ensureSearcher() {
        try {
            if (searcher == null || reader == null) {
                if (reader != null) {
                    reader.close();
                }
                reader = DirectoryReader.open(directory);
                searcher = new IndexSearcher(reader);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Error refreshing searcher", ex);
        }
    }


    public List<Map<String, Object>> searchByJoinKeys(Collection<String> joinKeys) {
        ensureSearcher();

        List<Map<String, Object>> fieldMaps = new ArrayList<>();

        try {
            List<String> filteredKeys = joinKeys.stream().filter(Objects::nonNull).filter(joinKey -> bloomFilter.mightContain(joinKey)).toList();
            System.out.println("Join keys count: " + joinKeys.size() + ", filtered keys count: " + filteredKeys.size());

            for (List<String> partition : Lists.partition(filteredKeys, 500)) {
                BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

                for (String joinKey : partition) {
                    booleanQueryBuilder.add(new TermQuery(new Term("_join_key", joinKey)), BooleanClause.Occur.SHOULD);
                }

                Query query = booleanQueryBuilder.build();
                TopDocs topDocs = searcher.search(query, 10000);

                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    Document document = searcher.storedFields().document(scoreDoc.doc);
                    fieldMaps.add(documentToMap(document));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Search failed", e);
        }

        return fieldMaps;
    }

    private Map<String, Object> documentToMap(Document doc) {
        Map<String, Object> map = new HashMap<>();
        for (IndexableField field : doc.getFields()) {
            map.put(field.name(), doc.get(field.name()));
        }
        return map;
    }

    /**
     * Search documents by join key using Bloom filter optimization
     */
    public List<Document> searchByJoinKey(String joinKey) {
        // First check Bloom filter to avoid unnecessary searches
        if (!bloomFilter.mightContain(joinKey)) {
            return Collections.emptyList();
        }

        ensureSearcher();

        try {
            // Search for documents with matching join key
            Query query = new TermQuery(new Term("_join_key", joinKey));
            TopDocs topDocs = searcher.search(query, 10000);

            List<Document> results = new ArrayList<>();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                results.add(searcher.storedFields().document(scoreDoc.doc));
            }

            return results;
        } catch (Exception e) {
            throw new RuntimeException("Search failed", e);
        }
    }

    public BloomFilter<String> getBloomFilter() {
        return bloomFilter;
    }

    /**
     * Extract all unique join keys for Bloom filter construction
     */
    //TODO Update bloom filter after delete - extract join keys, etc
    public Set<String> extractJoinKeys() {
        try {
            IndexReader reader = DirectoryReader.open(writer);
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
        if (reader != null) {
            reader.close();
        }
        writer.close();
        directory.close();

        System.out.println("Index "+this.indexPath + " closed");
    }

    public Path getIndexPath() {
        return indexPath;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PlainLuceneIndex index = (PlainLuceneIndex) o;
        return Objects.equals(indexPath, index.indexPath);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(indexPath);
    }
}