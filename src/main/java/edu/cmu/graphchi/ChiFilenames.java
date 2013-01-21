package edu.cmu.graphchi;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class ChiFilenames {

    public static String vertexDataSuffix = "";

    public static String getFilenameOfVertexData(String baseFilename, BytesToValueConverter valueConv, boolean sparse) {
        return baseFilename + "." + valueConv.sizeOf() + "B.vout" + vertexDataSuffix  + (sparse ? ".sparse" : "");
    }

    public static String getFilenameOfDegreeData(String baseFilename, boolean sparse) {
        return baseFilename + "_degs.bin" + (sparse ? ".sparse" : "");
    }

    public static String getPartStr(int p, int nShards) {
        return "." + p + "_" + nShards;
    }

    public static String getDirnameShardEdataBlock(String edataShardName, int blocksize) {
        return edataShardName + "_blockdir_" + blocksize;
    }

    public static String getFilenameShardEdataBlock(String edataShardname, int blockId, int blocksize) {
        return getDirnameShardEdataBlock(edataShardname, blocksize) + "/" + blockId;
    }

    public static int getShardEdataSize(String edataShardname) throws IOException {
        String fname = edataShardname + ".size";
        BufferedReader rd = new BufferedReader(new FileReader(new File(fname)));
        String ln = rd.readLine();
        rd.close();
        return Integer.parseInt(ln);
    }

    public static String getFilenameShardEdata(String baseFilename, BytesToValueConverter valueConv, int p, int nShards) {
        return baseFilename + ".edata_azv.e" + valueConv.sizeOf() + "B." + p + "_" + nShards;
    }

    public static String getFilenameShardsAdj(String baseFilename, int p, int nShards) {
        return baseFilename + ".edata_azv." + p + "_" + nShards + ".adj";
    }

    public static String getFilenameIntervals(String baseFilename, int nShards) {
        return baseFilename + "." + nShards + ".intervals";
    }

    public static String getVertexTranslateDefFile(String baseFilename, int nshards) {
        return baseFilename + "." + nshards + ".vtranslate";
    }

    public static int getBlocksize(int sizeOf) {
        int blocksize = 4096 * 1024;
        while (blocksize % sizeOf != 0) blocksize++;
        assert(blocksize % sizeOf == 0);
        return blocksize;
    }

    // http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
    public static int getPid() {
        try {
            java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
            java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
            jvm.setAccessible(true);
            sun.management.VMManagement mgmt = (sun.management.VMManagement) jvm.get(runtime);
            java.lang.reflect.Method pid_method = mgmt.getClass().getDeclaredMethod("getProcessId");
            pid_method.setAccessible(true);
            return  (Integer) pid_method.invoke(mgmt);
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }
}

