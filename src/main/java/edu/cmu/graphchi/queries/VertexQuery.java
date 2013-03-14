package edu.cmu.graphchi.queries;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.auxdata.DegreeData;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import edu.cmu.graphchi.io.CompressedIO;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.vertexdata.VertexIdValue;
import ucar.unidata.io.RandomAccessFile;

/**
 * Disk-based queries of out-edges of a vertex.
 * @author Aapo Kyrola
 */
public class VertexQuery {

    private static final int NTHREADS = 4;

    private static final Logger logger = ChiLogger.getLogger("vertexquery");
    private ArrayList<Shard> shards;
    private ExecutorService executor;
    private String baseFilename;
    private int numShards;
    private int cacheSize;
    private Map<Integer, int[]> highIndegreeCache;
    private double cacheIndegreeFraction;

    public VertexQuery(String baseFilename, int numShards, int cacheSize, double cacheIndegreeFraction) throws IOException{
        shards = new ArrayList<Shard>();
        for(int i=0; i<numShards; i++) {
            shards.add(new Shard(baseFilename, i, numShards));
        }
        executor = Executors.newFixedThreadPool(NTHREADS);

        this.numShards = numShards;
        this.baseFilename = baseFilename;
        this.cacheSize = cacheSize;
        this.highIndegreeCache = Collections.synchronizedMap(new HashMap<Integer, int[]>(cacheSize));
        this.cacheIndegreeFraction = cacheIndegreeFraction;
        populateCache();
    }

    private void populateCache() throws IOException {
        long t = System.currentTimeMillis();
        int  numVertices = ChiFilenames.numVertices(baseFilename, numShards);
        DegreeData deg = new DegreeData(baseFilename);
        int chunk = 4096 * 1024;
        int indegreeLimit = (int) (numVertices * cacheIndegreeFraction);
        for(int i=0; i < numVertices; i += chunk) {
            int en = Math.min(numVertices - 1, i + chunk - 1);
            deg.load(i, en);
            for(int vid=i; vid <= en; vid++) {
                VertexDegree d = deg.getDegree(vid);
                if (d.inDegree >= indegreeLimit) {
                    // Not most efficient, but ok
                    HashSet<Integer> outEdges = queryOutNeighbors(vid);
                    int[] cached = new int[outEdges.size()];
                    int j = 0;
                    for(Integer nb : outEdges) {
                        cached[j++] = nb;
                    }
                    highIndegreeCache.put(vid, cached);

                    if (highIndegreeCache.size() >= cacheSize) {
                        logger.info("Cache size exceeded, stop populating.");
                        break;
                    }
                }
            }
        }
        logger.info("Cache population took " + (System.currentTimeMillis() - t) + " ms");
        logger.info("Cache size: " + highIndegreeCache.size() + ", limit was: " + indegreeLimit);
    }


    public HashMap<Integer, Integer> queryOutNeighbors(final Collection<Integer> _queryVertices) {
        HashMap<Integer, Integer> results;
        List<Future<HashMap<Integer, Integer>>> queryFutures = new ArrayList<Future<HashMap<Integer, Integer>>>();

        /* Check which ones are in cache */
        long st = System.currentTimeMillis();
        HashMap<Integer, Integer> fromCache = new HashMap<Integer, Integer>(1000000);
        final ArrayList<Integer> queryVertices = new ArrayList<Integer>(_queryVertices.size());
        int cacheHits = 0;
        for(Integer a : _queryVertices) {
            if (highIndegreeCache.containsKey(a)) {
                cacheHits++;
                int[] out = highIndegreeCache.get(a);
                for(Integer vid : out) {
                    if (fromCache.containsKey(vid)) {
                        fromCache.put(vid, fromCache.get(vid) + 1);
                    } else {
                        fromCache.put(vid, 1);
                    }
                }
            } else {
                queryVertices.add(a);
            }
        }
        logger.info("Cached queries took: " + (System.currentTimeMillis() - st));

        /* Execute queries in parallel */
        for(Shard shard : shards) {
            final Shard _shard = shard;
            queryFutures.add(executor.submit(new Callable<HashMap<Integer, Integer>>() {
                @Override
                public HashMap<Integer, Integer> call() throws Exception {
                    HashMap<Integer, Integer> edges = _shard.query(queryVertices);
                    return edges;
                }
            }));
        }

        /* Combine
        */
        try {
            results = fromCache;

            for(int i=0; i < queryFutures.size(); i++) {
                HashMap<Integer, Integer> shardResults = queryFutures.get(i).get();

                for(Map.Entry<Integer, Integer> e : shardResults.entrySet()) {
                    if (results.containsKey(e.getKey())) {
                        results.put(e.getKey(), e.getValue() + results.get(e.getKey()));
                    } else {
                        results.put(e.getKey(), e.getValue());
                    }
                }
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        logger.info("From cache: " + cacheHits + " / " + _queryVertices.size());

        return  results;
    }

    public HashSet<Integer> queryOutNeighbors(final int internalId) throws IOException  {
        if (highIndegreeCache.containsKey(internalId)) {
            int[] cached = highIndegreeCache.get(internalId);
            HashSet<Integer> cachedResult =  new HashSet<Integer>(cached.length);
            for(int j : cached) {
                cachedResult.add(j);
            }
            return cachedResult;
        }


        HashSet<Integer> friends;
        List<Future<HashSet<Integer>>> queryFutures = new ArrayList<Future<HashSet<Integer>>>();

        /* Query from shards in parallel */
        for(Shard shard : shards) {
            final Shard _shard = shard;
            queryFutures.add(executor.submit(new Callable<HashSet<Integer>>() {
                @Override
                public HashSet<Integer> call() throws Exception {
                    return _shard.query(internalId);
                }
            }));
        }
        try {
            friends = queryFutures.get(0).get();

            for(int i=1; i < queryFutures.size(); i++) {
                HashSet<Integer> shardFriends = queryFutures.get(i).get();
                for(Integer fr : shardFriends) {
                    friends.add(fr);
                }
            }
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
        return friends;
    }

    /**
     * Shutdowns the executor threads.
     */
    public void shutdown() {
        executor.shutdown();
    }


    static class Shard {
        RandomAccessFile adjFile;
        ShardIndex index;
        int shardNum;
        int numShards;
        String fileName;


        private Shard(String fileName, int shardNum, int numShards) throws IOException {
            this.shardNum = shardNum;
            this.numShards = numShards;
            this.fileName = fileName;
            File f = new File(ChiFilenames.getFilenameShardsAdj(fileName, shardNum, numShards));
            adjFile = new RandomAccessFile(f.getAbsolutePath(), "r", 64 * 1024);
            index = new ShardIndex(new File(f.getAbsolutePath() + ".index"));
        }

        /**
         * Query efficiently all vertices
         * @param queryIds
         * @return
         * @throws IOException
         */
        public HashMap<Integer, Integer> query(Collection<Integer> queryIds) throws IOException {
            /* Sort the ids because the index-entries will be in same order */
            ArrayList<Integer> sortedIds = new ArrayList<Integer>(queryIds);
            Collections.sort(sortedIds);

            ArrayList<IndexEntry> indexEntries = new ArrayList<IndexEntry>(sortedIds.size());
            for(Integer a : sortedIds) {
                indexEntries.add(index.lookup(a));
            }

            HashMap<Integer, Integer> results = new HashMap<Integer, Integer>(5000);
            IndexEntry entry = null, lastEntry = null;
            int curvid=0, adjOffset=0;
            for(int qIdx=0; qIdx < sortedIds.size(); qIdx++) {
                entry = indexEntries.get(qIdx);
                int vertexId = sortedIds.get(qIdx);

                /* If consecutive vertices are in same indexed block, i.e their
                   index entries are the same, then we just continue.
                 */
                if (qIdx == 0 || !entry.equals(lastEntry))   {
                    curvid = entry.vertex;
                    adjOffset = entry.fileOffset;
                    adjFile.seek(adjOffset);
                }
                while(curvid <= vertexId) {
                    int n;
                    int ns = adjFile.readUnsignedByte();
                    assert(ns >= 0);
                    adjOffset++;

                    if (ns == 0) {
                        curvid++;
                        int nz = adjFile.readUnsignedByte();

                        adjOffset++;
                        assert(nz >= 0);
                        curvid += nz;
                        continue;
                    }

                    if (ns == 0xff) {
                        n = adjFile.readInt();
                        adjOffset += 4;
                    } else {
                        n = ns;
                    }

                    if (curvid == vertexId) {
                        while (--n >= 0) {
                            int target = adjFile.readInt();
                            Integer curCount = results.get(target);
                            if (curCount == null) {
                                results.put(target, 1);
                            } else {
                                results.put(target, 1 + curCount);
                            }
                        }
                    } else {
                        adjFile.skipBytes(n * 4);
                    }
                    curvid++;
                }
            }
            return results;
        }

        public HashSet<Integer> query(int vertexId) throws IOException {
            return new HashSet<Integer>(query(Collections.singletonList(vertexId)).keySet());
        }

        public <VT> List<VertexIdValue<VT>> queryWithValues(int vertexId, BytesToValueConverter<VT> conv) throws
                IOException {

            List<VertexIdValue<VT>> results = new ArrayList<VertexIdValue<VT>>();
            IndexEntry entry = index.lookup(vertexId);

            int curvid = entry.vertex;
            int adjOffset = entry.fileOffset;
            int edgeOffset = entry.edgePointer;
            String edataShardName = ChiFilenames.getFilenameShardEdata(fileName, conv, shardNum, numShards);
            int fileSize = ChiFilenames.getShardEdataSize(edataShardName);


            adjFile.seek(adjOffset);

            /* Edge data block*/
            int blockSize = ChiFilenames.getBlocksize(conv.sizeOf());

            byte[] edgeDataBlock = new byte[blockSize];
            int curBlockId = (-1);
            byte[] tmp = new byte[conv.sizeOf()];

            while(curvid <= vertexId) {
                int n;
                int ns = adjFile.readUnsignedByte();
                assert(ns >= 0);
                adjOffset++;

                if (ns == 0) {
                    curvid++;
                    int nz = adjFile.readUnsignedByte();

                    adjOffset++;
                    assert(nz >= 0);
                    curvid += nz;
                    continue;
                }

                if (ns == 0xff) {
                    n = adjFile.readInt();
                    adjOffset += 4;
                } else {
                    n = ns;
                }

                if (curvid == vertexId) {
                    while (--n >= 0) {
                        int target = adjFile.readInt();

                        int blockId = edgeOffset * conv.sizeOf() / blockSize;
                        if (blockId != curBlockId) {
                            String blockFileName = ChiFilenames.getFilenameShardEdataBlock(
                                    edataShardName,
                                    blockId, blockSize);
                            curBlockId = blockId;
                            int len = Math.min(blockSize, fileSize - blockId * blockSize);
                            CompressedIO.readCompressed(new File(blockFileName), edgeDataBlock, len);
                        }
                        System.arraycopy(edgeDataBlock, (edgeOffset * conv.sizeOf()) % blockSize, tmp, 0, conv.sizeOf());
                        VT value = conv.getValue(tmp);
                        results.add(new VertexIdValue<VT>(target, value));
                        edgeOffset++;
                    }
                } else {
                    adjFile.skipBytes(n * 4);
                    edgeOffset += n;
                }
                curvid++;
            }
            return results;
        }


    }

    static class ShardIndex {
        File indexFile;
        int[] vertices;
        int[] edgePointer;
        int[] fileOffset;

        ShardIndex(File indexFile) throws IOException {
            this.indexFile = indexFile;
            load();
        }

        private void load() throws IOException {
            int n = (int) (indexFile.length() / 12) + 1;
            vertices = new int[n];
            edgePointer = new int[n];
            fileOffset = new int[n];

            vertices[0] = 0;
            edgePointer[0] = 0;
            fileOffset[0] = 0;

            DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile)));
            int i = 1;
            while (i < n) {
                vertices[i] = dis.readInt();
                fileOffset[i] = dis.readInt();
                edgePointer[i] = dis.readInt();
                i++;
            }
        }

        IndexEntry lookup(int vertexId) {
            int idx = Arrays.binarySearch(vertices, vertexId);
            if (idx >= 0) {
                return new IndexEntry(vertexId, edgePointer[idx], fileOffset[idx]);
            } else {
                idx = -(idx + 1) - 1;
                return new IndexEntry(vertices[idx], edgePointer[idx], fileOffset[idx]);
            }

        }

    }

    static class IndexEntry {
        int vertex, edgePointer, fileOffset;

        IndexEntry(int vertex, int edgePointer, int fileOffset) {
            this.vertex = vertex;
            this.edgePointer = edgePointer;
            this.fileOffset = fileOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IndexEntry that = (IndexEntry) o;

            if (edgePointer != that.edgePointer) return false;
            if (fileOffset != that.fileOffset) return false;
            if (vertex != that.vertex) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = vertex;
            result = 31 * result + edgePointer;
            result = 31 * result + fileOffset;
            return result;
        }

        public String toString() {
            return "vertex: " + vertex + ", offset=" + fileOffset;
        }
    }


}
