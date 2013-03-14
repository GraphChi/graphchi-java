package edu.cmu.graphchi.datablocks;

import java.util.ArrayList;

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
 * Manages large chunks of data which are accessed using ChiPointers.
 * Used internally by GraphChi.
 * @author akyrola
 */
public class DataBlockManager {

    private ArrayList<byte[]> blocks = new ArrayList<byte[]>(32678);

    public DataBlockManager() {

    }

    public int allocateBlock(int numBytes) {
        byte[] dataBlock = new byte[numBytes];

        synchronized(blocks) {
            int blockId = blocks.size();
            blocks.add(blockId, dataBlock);
            return blockId;
        }
    }

    public byte[] getRawBlock(int blockId) {
        byte[] bb = blocks.get(blockId);    /* Note, not synchronized! */
        if (bb == null) {
            throw new IllegalStateException("Null-reference!");
        }

        return bb;
    }


    /**
     * Called by the engine to clear the registry. All blocks must be null
     * prior to calling!
     */
    public void reset() {
        for(int i=0; i<blocks.size(); i++) {
            if (blocks.get(i) != null) {
                throw new RuntimeException("Tried to reset block manager, but it was non-empty at index: " + i);
            }
        }
        blocks.clear();
    }

    public boolean empty() {
        for(int i=0; i<blocks.size(); i++) {
            if (blocks.get(i) != null) {
                return false;
            }
        }
        return true;
    }

    public void release(int blockId) {
        blocks.set(blockId, null);
    }

    public <T> T dereference(ChiPointer ptr, BytesToValueConverter<T> conv) {
        byte[] arr = new byte[conv.sizeOf()];

        if (ptr == null) {
            throw new IllegalStateException("Tried to dereference a null pointer!");
        }

        System.arraycopy(getRawBlock(ptr.blockId), ptr.offset, arr, 0, arr.length);
        return conv.getValue(arr);
    }

    public <T> void writeValue(ChiPointer ptr, BytesToValueConverter<T> conv, T value) {
        byte[] arr = new byte[conv.sizeOf()];
        conv.setValue(arr, value);
        System.arraycopy(arr, 0, getRawBlock(ptr.blockId), ptr.offset, arr.length);
    }

    public <T> void writeValue(ChiPointer ptr, byte[] data) {
        System.arraycopy(data, 0, getRawBlock(ptr.blockId), ptr.offset, data.length);
    }
}
