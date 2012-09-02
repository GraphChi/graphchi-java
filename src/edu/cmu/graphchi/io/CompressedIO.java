package edu.cmu.graphchi.io;

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


import java.io.*;
import java.util.zip.*;

/**
 * Zlib-compressed I/O support.
 * @author akyrola
 */
public class CompressedIO {

	public static void readCompressed(File f, byte[] buf, int nbytes) throws FileNotFoundException, IOException {
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
		InflaterInputStream iis = new InflaterInputStream(bis);
		int a = 0;
		int read = 0;
		while (a > 0) {
			a = iis.read(buf, read, nbytes - read);
		}
		iis.close(); bis.close();
		
	}
	
	public static void writeCompressed(File f, byte[] data, int nbytes) throws FileNotFoundException, IOException {
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f));
		DeflaterOutputStream dos = new DeflaterOutputStream(bos);
		dos.write(data, 0, nbytes);
		dos.close(); bos.close();
	}
	
}
