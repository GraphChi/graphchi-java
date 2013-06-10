package edu.cmu.graphchi.apps.recommendations;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.queries.VertexQuery;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Emulates Twitter's Who-To-Follow (WTF) algorithm's SALSA part as described in WWW'13 paper
 * WTF: The Who to Follow Service at Twitter: http://www.stanford.edu/~rezab/papers/wtf_overview.pdf
 *
 * This demonstration loads the followers
 * of the "circle of trust" (top visited vertices in egocentric random walk) directly
 * from the shards using the edu.cmu.graphchi.queries.VertexQuery class.  Then SALSA
 * algorithm is run so that the "circle of trust" is on the left as "hubs" and their followers
 * on the right as "authorities".
 *
 * Circle of trust must be given externally to this class. <b>Note:</b> work in progress.
 * @author Aapo Kyrola
 */

// TODO: Make multithreaded / thread-safe.
public class CircleOfTrustSalsa {

    private static final Logger logger = ChiLogger.getLogger("circle-of-trust");


    static class SalsaVertex {
        int id;
        int degree = 0;
        SalsaVertex(int id) {
            this.id = id;
        }
        double value = 1.0;
        ArrayList<Integer> neighbors;
    }

    private VertexQuery queryService;

    // Neighbor list
    private HashMap<Integer, SalsaVertex> hubs;
    private HashMap<Integer, SalsaVertex> authorities;

    // Static cache
    static private Map<Integer, ArrayList<Integer>> cache;
    private String graphName;

    private static final int FILTER_LIMIT = 4;


    // NOTE: non-thread safe
    public CircleOfTrustSalsa(VertexQuery queryService, final int cacheSize) throws Exception {
        this.queryService = queryService;

        synchronized (CircleOfTrustSalsa.class) {
            if (cache == null) {
                cache = Collections.synchronizedMap(new LinkedHashMap<Integer, ArrayList<Integer>>(cacheSize, 1.0f, true) // LRU
                {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, ArrayList<Integer>> integerArrayListEntry) {
                        return this.size() > cacheSize;
                    }
                });
            }
        }
    }

    public void initializeGraph(Collection<Integer> circleOfTrust) {
        hubs = new HashMap<Integer, SalsaVertex>(circleOfTrust.size(), 1.0f);
        long t = System.currentTimeMillis();

        int totalNeighbors = 0;

        int cacheHits = 0;
        HashSet<Integer> querySet = new HashSet<Integer>(circleOfTrust.size());
        for(int v : circleOfTrust) {
            hubs.put(v, new SalsaVertex(v));
            if (cache.containsKey(v)) {
                SalsaVertex hub = hubs.get(v);
                hub.neighbors = cache.get(v);
                hub.degree = hub.neighbors.size();
                cacheHits++;
            } else {
                querySet.add(v);
            }
        }


        /* Load neighbors of the circle of trust -- we probably would need some limitation here?? */
        HashMap<Integer, ArrayList<Integer>> hubNeighbors = queryService.queryOutNeighbors(querySet);
        long queryTime = System.currentTimeMillis() - t;

        /* Initialize salsa */
        t = System.currentTimeMillis();

        for(Map.Entry<Integer, ArrayList<Integer>> entry: hubNeighbors.entrySet()) {
            int hubId = entry.getKey();
            SalsaVertex hub = hubs.get(hubId);
            hub.neighbors = entry.getValue();
            hub.degree = entry.getValue().size();
            cache.put(hubId, entry.getValue());
        }

        // Count total neighbors
        for(SalsaVertex hub : hubs.values()) {
            totalNeighbors += hub.neighbors.size();
        }

        long salsaInitTime0 = System.currentTimeMillis() - t;

        // We do not add neighbors to authorities -- we can push values to authorities
        // and pull to hubs.

        int[] authEntries = new int[totalNeighbors];

        int j = 0;
        for(SalsaVertex hub : hubs.values()) {
            for(int authId : hub.neighbors) {
                authEntries[j++] = authId;
            }
        }
        assert(j == authEntries.length);

        // Create map efficiently
        Arrays.sort(authEntries);

        int lastId = -1;
        int count = 0;
    //    int filtered = 0;

        ArrayList<SalsaVertex> tmpAuth = new ArrayList<SalsaVertex>(1 + authEntries.length / 100);
        for(int i=0; i < authEntries.length; i++) {
            int authId = authEntries[i];
            if (lastId != authId) {
                if (lastId >= 0) {
                    if (count > FILTER_LIMIT) {
                        SalsaVertex auth = new SalsaVertex(lastId);
                        auth.degree = count;
                        tmpAuth.add(auth);
                    } else {
                      //  filtered++;
                    }
                    count = 0;

                }
                lastId  = authId;
            }
            count++;

        }

        authorities = new HashMap<Integer, SalsaVertex>(tmpAuth.size());
        for(SalsaVertex auth : tmpAuth) {
            authorities.put(auth.id, auth);
        }

        // NOTE: remove neighbors!
            /*
        long salsaInitTime = System.currentTimeMillis() - t;
        logger.info("Query took: " + queryTime + " ms, circle of trust size=" + circleOfTrust.size() + ", cache size=" +
                cache.size() + ", hits=" + cacheHits);
        logger.info("Salsa init: " + salsaInitTime + " ms, first phase=" + salsaInitTime0 + " ms, hubs="
                + hubs.size() + ", auths=" + authorities.size());
        logger.info("Filtered: " + filtered);            */
    }

    /**
     * Compute SALSA on graph initialized in initializeGraph() method.
     * @param nIterations
     */
    public void computeSALSA(int nIterations) {
        for(int iter=0; iter < nIterations; iter++) {
            // Hubs: sum of authority-neighbors values divided by their degree
            for(SalsaVertex hub: hubs.values()) {
                double nbSum = 0.0;
                // Update the degree because not all authorities were selected
                int degree = 0;
                for(int authId : hub.neighbors) {
                    SalsaVertex auth = authorities.get(authId);
                    if (auth != null) {
                        nbSum += auth.value / auth.degree;
                        degree++;
                    }
                }
                hub.value = nbSum;
                hub.degree = degree;
            }

            // Authorities: push from authority side.
            // First: set values to zero
            for(SalsaVertex auth: authorities.values()) {
                auth.value = 0;
            }

            // Then, push hubs values to their auths
            for(SalsaVertex hub: hubs.values()) {
                double myContribution = hub.value / hub.degree;
                for(int authId : hub.neighbors) {
                    SalsaVertex auth = authorities.get(authId);
                    if (auth != null) {
                        auth.value += myContribution;
                    }
                }
            }
        }
    }

    public VertexQuery getQueryService() {
        return queryService;
    }

    /**
     * Return top K authorities (result from SALSA), but do not include users in the removeList
     * @param K
     * @param removeList
     * @return
     */
    public ArrayList<SalsaVertex> topAuthorities(int K, HashSet<Integer> removeList) {
        // TODO: faster top-K implementation
        ArrayList<SalsaVertex> all = new ArrayList<SalsaVertex>(authorities.size());
        all.addAll(authorities.values());
        Collections.sort(all, new Comparator<SalsaVertex>() {
            @Override
            public int compare(SalsaVertex salsaVertex, SalsaVertex salsaVertex1) {
                if (salsaVertex.value < salsaVertex1.value) return 1;
                else return (salsaVertex.value > salsaVertex1.value ? -1 : 0);
            }
        });

        ArrayList<SalsaVertex> result = new ArrayList<SalsaVertex>(K);
        int i = 0;
        while(result.size() < K) {
            if (i < all.size()) {
                SalsaVertex x = all.get(i);
                if (!removeList.contains(x.id))
                    result.add(x);
            } else {
                break;
            }
            i++;
        }
        return result;
    }

    public String namify(Integer value) throws IOException {
        File f = new File(graphName + "_names.dat");
        if (!f.exists()) {
            System.out.println("didn't find name file: " + f.getPath());
            return value+"";
        }
        int i = value * 16;
        RandomAccessFile raf = new RandomAccessFile(f.getAbsolutePath(), "r");
        raf.seek(i);
        byte[] tmp = new byte[16];
        raf.read(tmp);
        raf.close();
        return new String(tmp) + "(" + value + ")";
    }

    public static void main(String[] args) throws  Exception {
        String graphName = args[0];
        int nShards = Integer.parseInt(args[1]);

        CircleOfTrustSalsa csalsa = new CircleOfTrustSalsa(new VertexQuery(graphName, nShards), 10000);

        VertexIdTranslate vertexTrans = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(graphName, nShards)));

        BufferedReader cmdIn = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            System.out.print("Enter vertex id to query >> :: ");
            String ln = cmdIn.readLine();
            int vertex = Integer.parseInt(ln);

            // Circle of trust is just the vertex's followers for now
            HashSet<Integer> circle = csalsa.queryService.queryOutNeighbors(vertexTrans.forward(vertex));

            int maxCircleSize = 300;
            // max 500
            if (circle.size() > maxCircleSize) {
                int[] all = new int[circle.size()];
                int i = 0;
                for(Integer v : circle) all[i++] = v;
                HashSet<Integer> filteredCircle = new HashSet<Integer>();
                Random r  = new Random(260379);
                for(i=0; i < maxCircleSize; i++) filteredCircle.add(all[Math.abs(r.nextInt()) % all.length]);
                circle = filteredCircle;
            }

            csalsa.initializeGraph(circle);

            long t = System.currentTimeMillis();
            csalsa.computeSALSA(3);
            logger.info("SALSA computation took " + (System.currentTimeMillis() - t) + "ms");

            circle.add(vertexTrans.forward(vertex));
            ArrayList<SalsaVertex> top = csalsa.topAuthorities(20, circle);
            int j = 1;
            for(SalsaVertex sv : top) {
                int originalId = vertexTrans.backward(sv.id);
                logger.info("Top " + (j++) + " = " + originalId + " " + csalsa.namify(originalId) + " (" + sv.value + ")");
            }

        }
    }

}
