/** This class is intended for high performance I/O in scientific applications.
 * It combines the functionality of the BufferedInputStream and the
 * DataInputStream as well as more efficient handling of arrays.
 * This minimizes the number of method calls that are required to
 * read data.  Informal tests of this method show that it can
 * be as much as 10 times faster than using a DataInputStream layered
 * on a BufferedInputStream for writing large arrays.  The performance
 * gain on scalars or small arrays will be less but there should probably
 * never be substantial degradation of performance.
 *
 * One routine is added to the public interface of DataInput, readPrimitiveArray.
 * This routine provides efficient protocols for writing arrays.  Note that
 * they will create temporaries of a size equal to the array (if the array
 * is one dimensional).
 *
 * Note that there is substantial duplication of code to minimize method
 * invocations.  E.g., the floating point read routines read the data
 * as integer values and then convert to float.  However the integer
 * code is duplicated rather than invoked.
 */

// Member of the utilities package.

package nom.tam.util;

/* Copyright: Thomas McGlynn 1997-1998.
 * This code may be used for any purpose, non-commercial
 * or commercial so long as this copyright notice is retained
 * in the source code or included in or referred to in any
 * derived software.
 */

// What do we use in here?

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;

public class BufferedDataInputStream
        extends BufferedInputStream
        implements DataInput {


    private long bufferOffset=0;
    private int primitiveArrayCount;

    /** Use the BufferedInputStream constructor
     */
    public BufferedDataInputStream(InputStream o) {
        super(o);
    }
    /** Use the BufferedInputStream constructor
     */
    public BufferedDataInputStream(InputStream o, int bufLength) {
        super(o, bufLength);
    }


    public int read(byte[] buf, int offset, int len) throws IOException {
       int total = 0;

        // Ensure that the entire buffer is read.
        while (len > 0) {
            int xlen = super.read(buf, offset+total, len);
            if (xlen <= 0) {
                if (total == 0) {
                    throw new EOFException();
                } else {
                    return total;
                }
            } else {
                len -= xlen;
                total += xlen;
            }
        }
        return total;

    }

    public int read() throws IOException {
        return super.read();
    }

    public long skip(long offset) throws IOException {

        long total = 0;

        while (offset > 0) {
            long xoff = super.skip(offset);
            if (xoff == 0) {
                return total;
            }
            offset -= xoff;
            total += xoff;
        }
        return total;
    }

    public boolean readBoolean() throws IOException {

        int b = read();
        if (b == 1) {
            return true;
        } else {
            return false;
        }
    }

    public byte readByte() throws IOException {
        return (byte) read();
    }

    public int readUnsignedByte() throws IOException {
        return read() & 0x00ff;
    }

    public int readInt() throws IOException {
        byte[] b = new byte[4];

        if (read(b, 0, 4) < 4 ) {
            throw new EOFException();
        }
        int i = b[0] << 24  | (b[1]&0xFF) << 16 | (b[2]&0xFF) << 8 | (b[3]&0xFF);
        return i;
    }


    byte[] savedIntByte = new byte[4]; // assume single thread
    // Added by Aapo Kyrola
    public int readIntReversed() throws IOException {
        read(savedIntByte, 0, 4);
        //if (n < 4) throw new EOFException();    // Removed EOF exception
        int i = savedIntByte[3] << 24  | (savedIntByte[2]&0xFF) << 16 | (savedIntByte[1]&0xFF) << 8 | (savedIntByte[0]&0xFF);
        return i;
    }



    public short readShort() throws IOException {
        byte[] b = new byte[2];

        if (read(b, 0, 2) < 2) {
            throw new EOFException();
        }

        short s = (short) (b[0] << 8 | (b[1]&0xFF));
        return s;
    }

    public int readUnsignedShort() throws IOException {
        byte[] b = new byte[2];

        if (read(b,0,2) < 2) {
            throw new EOFException();
        }

        return (b[0]&0xFF) << 8  |  (b[1]&0xFF);
    }


    public char readChar() throws IOException {
        byte[] b = new byte[2];

        if (read(b, 0, 2)  <  2) {
            throw new EOFException();
        }

        char c = (char) (b[0] << 8 | (b[1]&0xFF));
        return c;
    }

    public long readLong() throws IOException {
        byte[] b = new byte[8];

        // Let's use two int's as intermediarys so that we don't
        // have a lot of casts of bytes to longs...
        if (read(b, 0, 8) < 8) {
            throw new EOFException();
        }
        int i1 =  b[0] << 24 | (b[1]&0xFF) << 16 | (b[2]&0xFF) << 8 | (b[3]&0xFF);
        int i2 =  b[4] << 24 | (b[5]&0xFF) << 16 | (b[6]&0xFF) << 8 | (b[7]&0xFF);
        return  (((long) i1) << 32) | (((long)i2)&0x00000000ffffffffL);
    }

    public float readFloat() throws IOException {

        byte[] b = new byte[4];   // Repeat this from readInt to save method call.
        if (read(b, 0, 4) < 4) {
            throw new EOFException();
        }

        int i = b[0] << 24  | (b[1]&0xFF) << 16 | (b[2]&0xFF) << 8 | (b[3]&0xFF);
        return Float.intBitsToFloat(i);

    }

    public double readDouble() throws IOException {

        byte[] b = new byte[8];
        if (read(b, 0, 8) < 8) {
            throw new EOFException();
        }

        int i1 =  b[0] << 24 | (b[1]&0xFF) << 16 | (b[2]&0xFF) << 8 | (b[3]&0xFF);
        int i2 =  b[4] << 24 | (b[5]&0xFF) << 16 | (b[6]&0xFF) << 8 | (b[7]&0xFF);

        return Double.longBitsToDouble( ((long) i1) << 32 | ((long)i2&0x00000000ffffffffL) );
    }

    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {

        if (off < 0 || len < 0 || off+len > b.length) {
            throw new IOException("Attempt to read outside byte array");
        }

        if (read(b, off, len) < len) {
            throw new EOFException();
        }
    }

    public int skipBytes(int toSkip) throws IOException {

        if (skip(toSkip) < toSkip) {
            throw new EOFException();
        } else {
            return toSkip;
        }
    }


    public String readUTF() throws IOException{

        // Punt on this one and use DataInputStream routines.
        DataInputStream d = new DataInputStream(this);
        return d.readUTF();

    }

    /** This routine uses the deprecated DataInputStream.readLine() method.
     * However, we really want to simulate the behavior of that
     * method so that's what we used.
     * @deprecated
     */
    public String readLine() throws IOException {
        // Punt on this and use DataInputStream routines.
        DataInputStream d = new DataInputStream(this);
        return d.readLine();
    }

    /** This routine provides efficient reading of arrays of any primitive type.
     * It is an error to invoke this method with an object that is not an array
     * of some primitive type.  Note that there is no corresponding capability
     * to writePrimitiveArray in BufferedDataOutputStream to read in an
     * array of Strings.
     *
     * @param o  The object to be read.  It must be an array of a primitive type,
     *           or an array of Object's.
     */

    public int readPrimitiveArray(Object o) throws IOException {

        // Note that we assume that only a single thread is
        // doing a primitive Array read at any given time.  Otherwise
        // primitiveArrayCount can be wrong and also the
        // input data can be mixed up.  If this assumption isn't
        // true we need to synchronize this call.

        primitiveArrayCount = 0;
        return primitiveArrayRecurse(o);
    }

    protected int primitiveArrayRecurse(Object o) throws IOException {

        if (o == null) {
            return primitiveArrayCount;
        }

        String className = o.getClass().getName();

        if (className.charAt(0) != '[') {
            throw new IOException("Invalid object passed to BufferedDataInputStream.readArray:"+className);
        }

        // Is this a multidimensional array?  If so process recursively.
        if (className.charAt(1) == '[') {
            for (int i=0; i < ((Object[])o).length; i += 1) {
                primitiveArrayRecurse(((Object[])o)[i]);
            }
        } else {

            // This is a one-d array.  Process it using our special functions.
            switch (className.charAt(1)) {
                case 'Z':
                    primitiveArrayCount += readBooleanArray((boolean[])o);
                    break;
                case 'B':
                    int len=read((byte[])o, 0, ((byte[])o).length);
                    if (len < ((byte[])o).length){
                        primitiveArrayCount += len;
                        primitiveEOFThrower();
                    }
                    primitiveArrayCount += len;
                    break;
                case 'C':
                    primitiveArrayCount += readCharArray((char[])o);
                    break;
                case 'S':
                    primitiveArrayCount += readShortArray((short[])o);
                    break;
                case 'I':
                    primitiveArrayCount += readIntArray((int[])o);
                    break;
                case 'J':
                    primitiveArrayCount += readLongArray((long[])o);
                    break;
                case 'F':
                    primitiveArrayCount += readFloatArray((float[])o);
                    break;
                case 'D':
                    primitiveArrayCount += readDoubleArray((double[])o);
                    break;
                case 'L':

                    // Handle an array of Objects by recursion.  Anything
                    // else is an error.
                    if (className.equals("[Ljava.lang.Object;") ) {
                        for (int i=0; i < ((Object[])o).length; i += 1) {
                            primitiveArrayRecurse( ((Object[]) o)[i] );
                        }
                    } else {
                        throw new IOException("Invalid object passed to BufferedDataInputStream.readArray: "+className);
                    }
                    break;
                default:
                    throw new IOException("Invalid object passed to BufferedDataInputStream.readArray: "+className);
            }
        }
        return primitiveArrayCount;
    }

    protected int readBooleanArray(boolean[] b) throws IOException {
        byte[] bx = new byte[b.length];

        if (read(bx, 0, bx.length) < bx.length) {
            primitiveEOFThrower();
        }

        for (int i=0; i < b.length; i += 1) {
            if (bx[i] == 1) {
                b[i] = true;
            } else {
                b[i] = false;
            }
        }
        return bx.length;
    }

    protected int readShortArray(short[] s) throws IOException {

        if (s.length == 0) {
            return 0;
        }

        byte[] b = new byte[2*s.length];

        if (read(b, 0, b.length) < b.length) {
            primitiveEOFThrower();
        }
        char c = (char) (b[0] << 8 | b[1]);

        for(int i=0; i<s.length; i += 1) {
            s[i] = (short) (b[2*i] << 8 | (b[2*i+1]&0xFF));
        }
        return b.length;
    }

    protected int readCharArray(char[] c) throws IOException {
        byte[] b = new byte[2*c.length];
        if (read(b, 0, b.length) < b.length) {
            primitiveEOFThrower();
        }

        for(int i=0; i<c.length; i += 1) {
            c[i] = (char) (b[2*i] << 8 | (b[2*i+1]&0xFF));
        }
        return b.length;
    }

    protected int readIntArray(int[] i) throws IOException {
        byte[] b = new byte[4*i.length];

        if (read(b, 0, b.length) < b.length) {
            primitiveEOFThrower();
        }


        for (int ii=0; ii<i.length; ii += 1) {
            i[ii] = b[4*ii] << 24 | (b[4*ii+1]&0xFF) << 16 | (b[4*ii+2]&0xFF) << 8 | (b[4*ii+3]&0xFF);
        }
        return b.length;
    }

    protected int readLongArray(long[] l) throws IOException {
        byte[] b = new byte[8*l.length];

        if (read(b, 0, b.length) < b.length) {
            primitiveEOFThrower();
        }

        for (int i=0; i<l.length; i += 1) {
            int i1  = b[8*i]   << 24 | (b[8*i+1]&0xFF) << 16 | (b[8*i+2]&0xFF) << 8 | (b[8*i+3]&0xFF);
            int i2  = b[8*i+4] << 24 | (b[8*i+5]&0xFF) << 16 | (b[8*i+6]&0xFF) << 8 | (b[8*i+7]&0xFF);
            l[i] = ( (long) i1) << 32 | ((long)i2&0x00000000FFFFFFFFL);
        }
        return b.length;
    }

    protected int readFloatArray(float[] f) throws IOException {

        byte[] b = new byte[4*f.length];

        if (read(b, 0, b.length) < b.length) {
            primitiveEOFThrower();
        }

        for (int i=0; i<f.length; i += 1) {
            int t = b[4*i] << 24 |
                    (b[4*i+1]&0xFF) << 16 |
                    (b[4*i+2]&0xFF) <<  8 |
                    (b[4*i+3]&0xFF);
            f[i] = Float.intBitsToFloat(t);
        }
        return b.length;
    }

    protected int readDoubleArray(double[] d) throws IOException {
        byte[] b = new byte[8*d.length];

        if(read(b, 0, b.length) < b.length) {
            primitiveEOFThrower();
        }

        for (int i=0; i<d.length; i += 1) {
            int i1  = b[8*i]   << 24 | (b[8*i+1]&0xFF) << 16 | (b[8*i+2]&0xFF) << 8 | (b[8*i+3]&0xFF);
            int i2  = b[8*i+4] << 24 | (b[8*i+5]&0xFF) << 16 | (b[8*i+6]&0xFF) << 8 | (b[8*i+7]&0xFF);
            d[i] = Double.longBitsToDouble(
                    ((long) i1) << 32 | ((long)i2&0x00000000FFFFFFFFL));
        }
        return b.length;
    }

    protected void primitiveEOFThrower() throws EOFException {
        throw new EOFException("EOF on primitive array read after "+primitiveArrayCount+" bytes.");
    }

    public void printStatus() {

        System.out.println("BufferedDataInputStream:");
        System.out.println("    count="+count);
        System.out.println("      pos="+pos);
    }

    public String toString() {
        return "BufferedDataInputStream[count="+count+",pos="+pos+"]";
    }



}