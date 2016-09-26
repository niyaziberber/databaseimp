package simpledb.file;

import simpledb.server.SimpleDB;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * The contents of a disk block in memory.
 * A page is treated as an array of BLOCK_SIZE bytes.
 * There are methods to get/set values into this array,
 * and to read/write the contents of this array to a disk block.
 *
 * For an example of how to use Page and
 * {@link Block} objects,
 * consider the following code fragment.
 * The first portion increments the integer at offset 792 of block 6 of file junk.
 * The second portion stores the string "hello" at offset 20 of a page,
 * and then appends it to a new block of the file.
 * It then reads that block into another page
 * and extracts the value "hello" into variable s.
 * <pre>
 * Page p1 = new Page();
 * Block blk = new Block("junk", 6);
 * p1.read(blk);
 * int n = p1.getInt(792);
 * p1.setInt(792, n+1);
 * p1.write(blk);
 *
 * Page p2 = new Page();
 * p2.setString(20, "hello");
 * blk = p2.append("junk");
 * Page p3 = new Page();
 * p3.read(blk);
 * String s = p3.getString(20);
 * </pre>
 * @author Edward Sciore
 */
public class Page {
    /**
     * The number of bytes in a block.
     * This value is set unreasonably low, so that it is easier
     * to create and test databases having a lot of blocks.
     * A more realistic value would be 4K.
     */
    public static final int BLOCK_SIZE = 400;

    /**
     * The size of an integer in bytes.
     * This value is almost certainly 4, but it is
     * a good idea to encode this value as a constant.
     */
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    /**
     * The size of a character in bytes.
     * This value is almost certainly 2, but it is
     * a good idea to encode this value as a constant.
     */
    public static final int CHARACTER_SIZE = Character.SIZE / Byte.SIZE;

    /**
     * The charset to be used. Here we define it to be ASCII,
     * but it can be changed as necessary.
     */
    private static final Charset SDBcharset = StandardCharsets.US_ASCII;

    /**
     * The maximum size, in bytes, of a string of length n.
     * A string is represented as the characters followed by \0 char delimiter.
     * each String should then have n * 2 + 2, or (n+1) * 2 size.
     * @param n the size of the string
     * @return the maximum number of bytes required to store a string of size n
     */
    public static final int STR_SIZE(int n) {
        return (n+1) * CHARACTER_SIZE;
    }

    private ByteBuffer contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
    private FileMgr filemgr = SimpleDB.fileMgr();

    /**
     * Creates a new page.  Although the constructor takes no arguments,
     * it depends on a {@link FileMgr} object that it gets from the
     * method {@link SimpleDB#fileMgr()}.
     * That object is created during system initialization.
     * Thus this constructor cannot be called until either
     * {@link SimpleDB#init(String)} or
     * {@link SimpleDB#initFileMgr(String)} or
     * {@link SimpleDB#initFileAndLogMgr(String)} or
     * {@link SimpleDB#initFileLogAndBufferMgr(String)}
     * is called first.
     */
    public Page() {}

    /**
     * Populates the page with the contents of the specified disk block.
     * @param blk a reference to a disk block
     */
    public synchronized void read(Block blk) {
        filemgr.read(blk, contents);
    }

    /**
     * Writes the contents of the page to the specified disk block.
     * @param blk a reference to a disk block
     */
    public synchronized void write(Block blk) {
        filemgr.write(blk, contents);
    }

    /**
     * Appends the contents of the page to the specified file.
     * @param filename the name of the file
     * @return the reference to the newly-created disk block
     */
    public synchronized Block append(String filename) {
        return filemgr.append(filename, contents);
    }

    /**
     * Returns the integer value at a specified offset of the page.
     * If an integer was not stored at that location,
     * the behavior of the method is unpredictable.
     * @param offset the byte offset within the page
     * @return the integer value at that offset
     */
    public synchronized int getInt(int offset) {
        try {
            contents.position(offset);
            return contents.getInt();
        }
        catch(Exception e) {
            System.out.println("Exception offset =" + offset);
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Writes an integer to the specified offset on the page.
     * @param offset the byte offset within the page
     * @param val the integer to be written to the page
     */
    public synchronized void setInt(int offset, int val) {
        contents.position(offset);
        contents.putInt(val);
    }

    /**
     * Returns the string value at the specified offset of the page.
     * If a string was not stored at that location,
     * the behavior of the method is unpredictable.
     * @param offset the byte offset within the page
     * @return the string value at that offset
     */
    public synchronized String getString(int offset) {
        contents.position(offset);
        String s = "";
        char c;
        while ((c = contents.getChar()) != '\0') {
            s += c;
        }
        return s;
    }

    /**
     * Writes a string to the specified offset on the page.
     * @param offset the byte offset within the page
     * @param val the string to be written to the page
     */
    public synchronized void setString(int offset, String val) {
        contents.position(offset);
        for (char c: val.toCharArray()) {
            contents.putChar(c);
        }
        contents.putChar('\0');
        //  System.out.println("Length of " + val + " = " + byteval.length);
    }
}
