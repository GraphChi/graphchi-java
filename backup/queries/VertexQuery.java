package edu.cmu.graphchi.queries;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.engine.auxdata.DegreeData;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import edu.cmu.graphchi.io.CompressedIO;
import edu.cmu.graphchi.shards.ShardIndex;
import edu.cmu.graphchi.vertexdata.VertexIdValue;
import ucar.unidata.io.RandomAccessFile;

/**
 * Disk-based queries of out-edges of a vertex.
 * <b>Note:</b> all vertex-ids in *internal* vertex id space.
 * @author Aapo Kyrola
 */
public class VertexQuery {

    private static final int NTHREADS = 4;

    private static final Logger logger = ChiLogger.getLogger("vertexquery");
    private ArrayList<Shard> shards;
    private ExecutorService executor;

    public VertexQuery(String baseFilename, int numShards) throws IOException{
        shards = new ArrayList<Shard>();
        for(int i=0; i<numShards; i++) {
            shards.add(new Shard(baseFilename, i, numShards));
        }
        executor = Executors.newFixedThreadPool(NTHREADS);
    }


    /**
     * Queries all out neighbors of given vertices and returns a hashmap with (vertex-id, count),
     * where count is the number of queryAndCombine vertices who had the vertex-id as neighbor.
     * @param queryVertices
     * @return
     */
    public HashMap<Integer, Integer> queryOutNeighborsAndCombine(final Collection<Integer> queryVertices) {
        HashMap<Integer, Integer> results;
        List<Future<HashMap<Integer, Integer>>> queryFutures = new ArrayList<Future<HashMap<Integer, Integer>>>();

        /* Check which ones are in cache */
        long st = System.currentTimeMillis();
        HashMap<Integer, Integer> fromCache = new HashMap<Integer, Integer>(1000000);

        logger.info("Cached queries took: " + (System.currentTimeMillis() - st));

        /* Execute queries in parallel */
        for(Shard shard : shards) {
            final Shard _shard = shard;
            queryFutures.add(executor.submit(new Callable<HashMap<Integer, Integer>>() {
                @Override
                public HashMap<Integer, Integer> call() throws Exception {
                    HashMap<Integer, Integer> edges = _shard.queryAndCombine(queryVertices);
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


        return  results;
    }

    /**
     * Queries out=neighbors for a given set of vertices.
     * @param queryVertices
     * @return
     */
    public HashMap<Integer, ArrayList<Integer>> queryOutNeighbors(final Collection<Integer> queryVertices) {
        HashMap<Integer,  ArrayList<Integer>> results;
        List<Future<HashMap<Integer, ArrayList<Integer>> >> queryFutures
                = new ArrayList<Future<HashMap<Integer, ArrayList<Integer>> >>();

        /* Check which ones are in cache */
        long st = System.currentTimeMillis();
        HashMap<Integer, ArrayList<Integer>> fromCache = new HashMap<Integer, ArrayList<Integer>>(1000);


        /* Execute queries in parallel */
        for(Shard shard : shards) {
            final Shard _shard = shard;
            queryFutures.add(executor.submit(new Callable<HashMap<Integer, ArrayList<Integer>>>() {
                @Override
                public HashMap<Integer, ArrayList<Integer>> call() throws Exception {
                    HashMap<Integer, ArrayList<Integer>>  edges = _shard.query(queryVertices);
                    return edges;
                }
            }));
        }

        /* Combine
        */
        try {
            results = fromCache;

            for(int i=0; i < queryFutures.size(); i++) {
                HashMap<Integer,  ArrayList<Integer>> shardResults = queryFutures.get(i).get();

                for(Map.Entry<Integer,  ArrayList<Integer>> e : shardResults.entrySet()) {
                    ArrayList<Integer> existing = results.get(e.getKey());
                    if (existing == null) {
                        results.put(e.getKey(), e.getValue());
                    } else {
                        existing.addAll(e.getValue());
                    }
                }
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        return  results;
    }


    /**
     * Return out-neighbors of given vertex
     * @param internalId
     * @return
     * @throws IOException
     */
    public HashSet<Integer> queryOutNeighbors(final int internalId) throws IOException  {
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
            index = new ShardIndex(f);
        }

        /**
         * Query efficiently all vertices
         * @param queryIds
         * @return
         * @throws IOException
         */
        public HashMap<Integer, Integer> queryAndCombine(Collection<Integer> queryIds) throws IOException {
            /* Sort the ids because the index-entries will be in same order */
            ArrayList<Integer> sortedIds = new ArrayList<Integer>(queryIds);
            Collections.sort(sortedIds);

            ArrayList<ShardIndex.IndexEntry> indexEntries = new ArrayList<ShardIndex.IndexEntry>(sortedIds.size());
            for(Integer a : sortedIds) {
                indexEntries.add(index.lookup(a));
            }

            HashMap<Integer, Integer> results = new HashMap<Integer, Integer>(5000);
            ShardIndex.IndexEntry entry = null, lastEntry = null;
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


        public HashMap<Integer, ArrayList<Integer>> query(Collection<Integer> queryIds) throws IOException {
            /* Sort the ids because the index-entries will be in same order */
            ArrayList<Integer> sortedIds = new ArrayList<Integer>(queryIds);
            Collections.sort(sortedIds);

            ArrayList<ShardIndex.IndexEntry> indexEntries = new ArrayList<ShardIndex.IndexEntry>(sortedIds.size());
            for(Integer a : sortedIds) {
                indexEntries.add(index.lookup(a));
            }

            HashMap<Integer, ArrayList<Integer>> results = new HashMap<Integer, ArrayList<Integer>>(queryIds.size());

            ShardIndex.IndexEntry entry = null, lastEntry = null;
            int curvid=0, adjOffset=0;
            for(int qIdx=0; qIdx < sortedIds.size(); qIdx++) {
                entry = indexEntries.get(qIdx);
                int vertexId = sortedIds.get(qIdx);

                 boolean found = false;

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
                        ArrayList<Integer> nbrs = new ArrayList<Integer>(n);
                        found = true;

                        while (--n >= 0) {
                            int target = adjFile.readInt();
                            nbrs.add(target);
                        }
                        results.put(vertexId, nbrs);
                    } else {
                        adjFile.skipBytes(n * 4);
                    }
                    curvid++;
                }
                if (!found) {
                    results.put(vertexId, new ArrayList<Integer>(0));
                }
            }
            return results;
        }

        public HashSet<Integer> query(int vertexId) throws IOException {
            return new HashSet<Integer>(query(Collections.singletonList(vertexId)).get(vertexId));
        }

        public <VT> List<VertexIdValue<VT>> queryWithValues(int vertexId, BytesToValueConverter<VT> conv) throws
                IOException {

            List<VertexIdValue<VT>> results = new ArrayList<VertexIdValue<VT>>();
            ShardIndex.IndexEntry entry = index.lookup(vertexId);

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


}
