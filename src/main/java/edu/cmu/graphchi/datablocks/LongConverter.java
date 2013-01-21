package edu.cmu.graphchi.datablocks;

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

/**
 * Converts byte[8] to Long and vice versa.
 * @author Aapo Kyrola
 */
public class LongConverter implements  BytesToValueConverter<Long> {
    public int sizeOf() {
        return 8;
    }

    public Long getValue(byte[] array) {
        return  ((long)(array[0]  & 0xff) << 56) +
                ((long)(array[1]  & 0xff) << 48) +
                ((long)(array[2] & 0xff) << 40) +
                ((long)(array[3] & 0xff) << 32) +
                ((long)(array[4]  & 0xff) << 24) +
                ((long)(array[5] & 0xff) << 16) +
                ((long)(array[6] & 0xff) << 8) +
                ((long)array[7] & 0xff);
    }

    public void setValue(byte[] array, Long x) {
        array[0] = (byte) ((x >>> 56) & 0xff);
        array[1] = (byte) ((x >>> 48) & 0xff);
        array[2] = (byte) ((x >>> 40) & 0xff);
        array[3] = (byte) ((x >>> 32) & 0xff);
        array[4] = (byte) ((x >>> 24) & 0xff);
        array[5] = (byte) ((x >>> 16) & 0xff);
        array[6] = (byte) ((x >>> 8) & 0xff);
        array[7] = (byte) ((x) & 0xff);
    }
}