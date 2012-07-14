package edu.cmu.graphchi;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;

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

    public static String getFilenameOfVertexData(String baseFilename, BytesToValueConverter valueConv) {
        return baseFilename + "." + valueConv.sizeOf() + "B.vout";
    }

    public static String getFilenameOfDegreeData(String baseFilename) {
        return baseFilename + "_degs.bin";
    }

    public static String getPartStr(int p, int nShards) {
        return "." + p + "_" + nShards;
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
}

