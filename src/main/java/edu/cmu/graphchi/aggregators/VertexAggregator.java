package edu.cmu.graphchi.aggregators;

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


import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;


/**
 * Compute aggregates over the vertex values.
 */
public class VertexAggregator {


    public static <VertexDataType> void  foreach(String baseFilename, BytesToValueConverter<VertexDataType> conv,
                                            ForeachCallback<VertexDataType> callback) throws IOException {

        File vertexDataFile = new File(ChiFilenames.getFilenameOfVertexData(baseFilename, conv));
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(vertexDataFile), 1024 * 1024);

        int i = 0;
        byte[] tmp = new byte[conv.sizeOf()];
        while (bis.available() > 0) {
            bis.read(tmp);
            VertexDataType value = conv.getValue(tmp);
            callback.callback(i, value);
            i++;
        }
        bis.close();

    }

}
