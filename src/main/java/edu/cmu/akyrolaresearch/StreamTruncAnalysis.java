package edu.cmu.akyrolaresearch;

import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.walks.distributions.DiscreteDistribution;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * Analyse the effect of the stream truncation.
 * @author Aapo Kyrola
 */
public class StreamTruncAnalysis {


    public static String hashKey(int vertexId, int maxElem) {
        return vertexId + "." + maxElem;
    }

    public static void main(String[] args) throws Exception {

        int bufferSize = Integer.parseInt(args[1]);
        int[] maxElements = {0, 64, 128, 256, 512, 1024, 2048};
        HashMap<Integer, ArrayList<Integer>> buffers = new HashMap<Integer, ArrayList<Integer>>();
        HashMap<String, FilteredDistribution> filteredDist = new HashMap<String, FilteredDistribution>();
        HashSet<Integer> uniqueVertices = new HashSet<Integer>();

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(args[0])));

        long nhops = 0;

        try {
            while(true)  {
                int source = dis.readInt();
                int at = dis.readInt();

                nhops ++;

               // if (nhops % 50000 == 0) System.out.println(nhops);

                if (!uniqueVertices.contains(source)) {
                    uniqueVertices.add(source);

                    for(int maxElem : maxElements) {
                        filteredDist.put(hashKey(source, maxElem), new FilteredDistribution(maxElem));
                    }
                    buffers.put(source, new ArrayList<Integer>(bufferSize));
                //    System.out.println("Added "+ source);
                }

                if (source != at) {

                    ArrayList<Integer> buffer = buffers.get(source);


                    buffer.add(at);
                    if (buffer.size() == bufferSize) {
                        for(int maxElem : maxElements) {
                            String cacheKey  = hashKey(source, maxElem);
                            FilteredDistribution d = filteredDist.get(cacheKey);
                            d.feed(buffer);
                        }
                        buffer.clear();
                      //  System.out.println("Drained " + source);
                    }

                }

            }
        } catch (EOFException err) {

        }
        System.out.println("Number of hops: " + nhops);

        FileOutputStream fos = new FileOutputStream("recalls_" + bufferSize + ".tsv");

        String header = "source\tmaxelem\ttop10count\tfulldistsize\tfilteredsize\tten\ttenrecall\ttwenty\ttwentyrecall\tthirty\tthirtyrecall" +
                  "\tforty\tfortyrecall\tfifty\tfiftyrecall\n";
        fos.write(header.getBytes());

        /* Analyze */
        for(Integer source : uniqueVertices) {
            System.out.println("Handle: " + source);
            DiscreteDistribution fullDist = filteredDist.get(hashKey(source, 0)).distr;

            if (fullDist.size() == 0) {
                System.out.println("No hops for " + source);
                continue;
            }

            ArrayList<IdCount[]> tops = new ArrayList<IdCount[]>();
            for(int i=1; i<=5; i++) {
                tops.add(fullDist.getTop(10 * i));
            }


            for(int maxElem : maxElements) {
                if (maxElem == 0) continue;
                String ckey = hashKey(source, maxElem);
                DiscreteDistribution filtered = filteredDist.get(ckey).distr;

                StringBuffer sb = new StringBuffer();
                sb.append(source + "\t" + maxElem + "\t" + tops.get(0)[tops.get(0).length - 1].count + "\t");
                sb.append(fullDist.size() + "\t" + filtered.size() + "\t");

                for(int i=1; i<=5; i++) {
                    int k = i * 10;
                    IdCount[] top = tops.get(i - 1);
                    int recall = 0;

                    IdCount[] filteredTop = filtered.getTop(k);

                    for(IdCount ic : top) {
                        if (idCountContains(filteredTop, ic.id)) recall++;
                    }

                    sb.append(k + "\t" + recall + "\t");
                }

                // how many recalled in total
             /*   sb.append(maxElem + "\t");
                int recall = 0;
                IdCount[] fullTop = fullDist.getTop(maxElem);
                for(IdCount ic: fullTop) {
                    if (filtered.getCount(ic.id) > 0) recall++;
                }
                sb.append(recall);
               */


                sb.append("\n");
                fos.write(sb.toString().getBytes());
            }
        }
    }

    private static boolean idCountContains(IdCount[] filteredTop, int id) {
        for(IdCount idc: filteredTop) {
            if (idc.id == id) return true;
        }
        return false;
    }


    static class FilteredDistribution {
        private DiscreteDistribution distr;
        private int maxElements;

        FilteredDistribution(int maxElements) {
            this.maxElements = maxElements;
            this.distr = new DiscreteDistribution(new int[0]);
        }

        void feed(ArrayList<Integer> hops) {
            int[] a = new int[hops.size()];
            for(int i=0; i < hops.size(); i++) {
                a[i] = hops.get(i);
            }
            Arrays.sort(a);

            DiscreteDistribution d = new DiscreteDistribution(a);
            this.distr = DiscreteDistribution.merge(this.distr, d);

            if (maxElements > 0) {
                int c = 0;
                while (this.distr.size() > maxElements) {
                    this.distr = this.distr.filteredAndShift(2);
                    c++;
                    if (c > 2) {
                        this.distr = this.distr.forceToSize(maxElements);
                        break;
                    }
                }
            }
        }



    }

}
