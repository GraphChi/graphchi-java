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
 * Internal representation for referring to a chunk of bytes.
 * Emulates C/C++ pointers.
 */
public class ChiPointer {
    public int blockId;
    public int offset;

    public ChiPointer(int blockId, int offset) {
        this.blockId = blockId;
        this.offset = offset;
    }

}
