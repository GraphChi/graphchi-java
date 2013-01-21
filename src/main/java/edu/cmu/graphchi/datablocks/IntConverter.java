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
 * Converts byte[4] ot Integer and vice versa.
 * @author  Aapo Kyrola
 */
public class IntConverter implements  BytesToValueConverter<Integer> {
    public int sizeOf() {
        return 4;
    }

    public Integer getValue(byte[] array) {
        return ((array[3]  & 0xff) << 24) + ((array[2] & 0xff) << 16) + ((array[1] & 0xff) << 8) + (array[0] & 0xff);
    }

    public void setValue(byte[] array, Integer x) {
        array[0] = (byte) ((x) & 0xff);
        array[1] = (byte) ((x >>> 8) & 0xff);
        array[2] = (byte) ((x >>> 16) & 0xff);
        array[3] = (byte) ((x >>> 24) & 0xff);

    }
}