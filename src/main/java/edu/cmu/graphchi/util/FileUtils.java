package edu.cmu.graphchi.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author Aapo Kyrola
 */
public class FileUtils {

    /**
     * Reads file completely and returns the contents as a string.
     * @param fileName
     * @return file contents
     * @throws IOException
     */
    public static String readToString(String fileName) throws IOException {
        File f =  new File(fileName);
        byte[] bytes = new byte[(int) f.length()];
        FileInputStream fis = new FileInputStream(f);

        int tot = 0;
        while(tot < bytes.length) {
          tot += fis.read(bytes, tot, bytes.length - tot);
        }

        fis.close();
        return new String(bytes);
    }
}
