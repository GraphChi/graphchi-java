package edu.cmu.graphchi.shards;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.DataBlockManager;

import java.io.*;
import java.util.ArrayList;
import edu.cmu.graphchi.io.*;

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
public class MemoryShard <EdgeDataType> {

	private String edgeDataFilename;
	private String adjDataFilename;
	private int rangeStart;
	private int rangeEnd;

	private byte[] adjData;
	private int[] blockIds = new int[0];
	private int[] blockSizes = new int[0];;

	private int edataFilesize;
	private boolean loaded = false;
	private boolean hasSetRangeOffset = false, hasSetOffset = false;

	private int rangeStartOffset, rangeStartEdgePtr, rangeContVid;

	private DataBlockManager dataBlockManager;
	private BytesToValueConverter<EdgeDataType> converter;
	private int streamingOffset, streamingOffsetEdgePtr, streamingOffsetVid;
	private int blocksize;

	private MemoryShard() {}

	public MemoryShard(String edgeDataFilename, String adjDataFilename, int rangeStart, int rangeEnd) {
		this.edgeDataFilename = edgeDataFilename;
		this.adjDataFilename = adjDataFilename;
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;

	}

	public void commitAndRelease(boolean modifiesInedges, boolean modifiesOutedges) throws IOException {
		int nblocks = blockIds.length;

		if (modifiesInedges) {
			int startStreamBlock = rangeStartEdgePtr / blocksize;
			for(int i=0; i < nblocks; i++) {
				String blockFilename = ChiFilenames.getFilenameShardEdataBlock(edgeDataFilename, i, blocksize);
				if (i >= startStreamBlock) {
					// Synchronous write
					CompressedIO.writeCompressed(new File(blockFilename), 
							dataBlockManager.getRawBlock(blockIds[i]),
							blockSizes[i]);
				} else {
					// Asynchronous write (not implemented yet, so is same as synchronous)
					CompressedIO.writeCompressed(new File(blockFilename), 
							dataBlockManager.getRawBlock(blockIds[i]),
							blockSizes[i]);
				}
			}

		} else if (modifiesOutedges) {
			int last = streamingOffsetEdgePtr;
			if (last == 0) {
				last = edataFilesize;
			}
			int startblock = (int) (rangeStartEdgePtr / blocksize);
			int endblock = (int) (last / blocksize);
			for(int i=startblock; i <= endblock; i++) {
				String blockFilename = ChiFilenames.getFilenameShardEdataBlock(edgeDataFilename, i, blocksize);
				CompressedIO.writeCompressed(new File(blockFilename), 
						dataBlockManager.getRawBlock(blockIds[i]),
						blockSizes[i]);
			}
		}
		/* Release all blocks */
		for(Integer blockId : blockIds) {
			dataBlockManager.release(blockId);
		}
	}

	public void loadVertices(int windowStart, int windowEnd, ChiVertex[] vertices)
			throws FileNotFoundException, IOException {
		if (adjData == null) {
			blocksize = ChiFilenames.getBlocksize(converter.sizeOf());
			loadAdj();
			loadEdata();
		}

		System.out.println("Load memory shard");
		int vid = 0;
		int edataPtr = 0;
		int adjOffset = 0;
		int sizeOf = converter.sizeOf();
		DataInputStream adjInput = new DataInputStream(new ByteArrayInputStream(adjData));
		while(adjInput.available() > 0) {
			if (!hasSetOffset && vid > rangeEnd) {
				streamingOffset = adjOffset;
				streamingOffsetEdgePtr = edataPtr;
				streamingOffsetVid = vid;
				hasSetOffset = true;
			}
			if (!hasSetRangeOffset && vid >= rangeStart) {
				rangeStartOffset = adjOffset;
				rangeStartEdgePtr = edataPtr;
				hasSetRangeOffset = true;
			}

			int n = 0;
			int ns = adjInput.readUnsignedByte();
			adjOffset += 1;
			assert(ns >= 0);
			if (ns == 0) {
				// next value tells the number of vertices with zeros
				vid++;
				int nz = adjInput.readUnsignedByte();
				adjOffset += 1;
				vid += nz;
				continue;
			}
			if (ns == 0xff) {   // If 255 is not enough, then stores a 32-bit integer after.
				n = Integer.reverseBytes(adjInput.readInt());
				adjOffset += 4;
			} else {
				n = ns;
			}

			ChiVertex vertex = null;
			if (vid >= windowStart && vid <= windowEnd) {
				vertex = vertices[vid - windowStart];
			}

			while (--n >= 0) {
				int target = Integer.reverseBytes(adjInput.readInt());
				adjOffset += 4;
				if (!(target >= rangeStart && target <= rangeEnd))
					throw new IllegalStateException("Target " + target + " not in range!");
				if (vertex != null) {
					vertex.addOutEdge(blockIds[edataPtr / blocksize], edataPtr % blocksize, target);
				}

				if (target >= windowStart) {
					if (target <= windowEnd) {
						ChiVertex dstVertex = vertices[target - windowStart];
						if (dstVertex != null) {
							dstVertex.addInEdge(blockIds[edataPtr / blocksize], edataPtr % blocksize, vid);
						}
						if (vertex != null && dstVertex != null) {
							dstVertex.parallelSafe = false;
							vertex.parallelSafe = false;
						}
					}
				}
				edataPtr += sizeOf;

				// TODO: skip
			}
			vid++;
		}


	}


	private void loadAdj() throws FileNotFoundException, IOException {
		File adjFile = new File(adjDataFilename);
		FileInputStream fis = new FileInputStream(adjFile);

		int filesize = (int) adjFile.length();
		adjData = new byte[filesize];

		int read = 0;
		while (read < filesize) {
			read += fis.read(adjData, read, filesize - read);
		}

	}

	private void loadEdata() throws FileNotFoundException, IOException {
		/* Load the edge data from file. Should be done asynchronously. */
		if (!loaded) {
			edataFilesize = ChiFilenames.getShardEdataSize(edgeDataFilename);
			int nblocks = edataFilesize / blocksize + (edataFilesize % blocksize == 0 ? 0 : 1);
			blockIds = new int[nblocks];
			blockSizes = new int[nblocks];
			for(int fileBlockId=0; fileBlockId < nblocks; fileBlockId++) {
				int fsize = Math.min(edataFilesize - blocksize * fileBlockId, blocksize);
				blockIds[fileBlockId] = dataBlockManager.allocateBlock(fsize);
				blockSizes[fileBlockId] = fsize;
				String blockfilename = ChiFilenames.getFilenameShardEdataBlock(edgeDataFilename, fileBlockId, blocksize);
				CompressedIO.readCompressed(new File(blockfilename), dataBlockManager.getRawBlock(blockIds[fileBlockId]), fsize);
			}

			loaded = true;
		}
	}

	public DataBlockManager getDataBlockManager() {
		return dataBlockManager;
	}

	public void setDataBlockManager(DataBlockManager dataBlockManager) {
		this.dataBlockManager = dataBlockManager;
	}

	public void setConverter(BytesToValueConverter<EdgeDataType> converter) {
		this.converter = converter;
	}

	public int getStreamingOffset() {
		return streamingOffset;
	}

	public int getStreamingOffsetEdgePtr() {
		return streamingOffsetEdgePtr;
	}

	public int getStreamingOffsetVid() {
		return streamingOffsetVid;
	}
}
