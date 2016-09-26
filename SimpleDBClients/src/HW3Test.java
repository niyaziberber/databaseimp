import simpledb.buffer.*;
import simpledb.server.SimpleDB;
import simpledb.file.*;
import java.util.*;

public class HW3Test {
   private static Map<Block,Buffer> buffs = new HashMap<Block, Buffer>();
   private static BufferMgr bm;
   
   public static void main(String args[]) throws Exception {
      SimpleDB.initFileLogAndBufferMgr("hw3db");
      
      bm = SimpleDB.bufferMgr();
      System.out.println(bm);
      
      pinBuffer(0); pinBuffer(1); pinBuffer(2); pinBuffer(3);
      pinBuffer(4); pinBuffer(5); pinBuffer(6); pinBuffer(7);
      System.out.println(bm);
      unpinBuffer(2); unpinBuffer(0); unpinBuffer(5); unpinBuffer(4);
      System.out.println(bm);
      pinBuffer(8); pinBuffer(9);
      System.out.println(bm);
      unpinBuffer(1); unpinBuffer(8); unpinBuffer(3); unpinBuffer(6);
      System.out.println(bm);
      pinBuffer(1); pinBuffer(10); pinBuffer(11); pinBuffer(12); pinBuffer(13); pinBuffer(14);
      System.out.println(bm);
   }
   
   private static void pinBuffer(int i) {
      Block blk = new Block("test", i);
      Buffer buff = bm.pin(blk);
      buffs.put(blk, buff);
      System.out.println("Pin block " + i);
   }
   
   private static void unpinBuffer(int i) {
      Block blk = new Block("test", i);
      Buffer buff = buffs.remove(blk);
      bm.unpin(buff);
      System.out.println("Unpin block " + i);
   }
}
