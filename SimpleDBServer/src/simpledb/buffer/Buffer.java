package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

public class Buffer {
   private Page contents = new Page();
   private Block blk = null;
   private int pins = 0;
   private int modifiedBy = -1;
   private int logSequenceNumber;
   
   private int id;  // new for HW3
   
   // a new constructor, for HW3
   public Buffer(int id) {
      this.id = id;
   }
   
   // a new method, for HW3
   public String toString() {
      String status = isPinned() ? " pinned" : " unpinned";
      return "Buffer " + id + ": contains " + blk + status;
   }
   
   public int getInt(int offset) {
      return contents.getInt(offset);
   }
   
   public String getString(int offset) {
      return contents.getString(offset);
   }
   
   public void setInt(int offset, int val, int txnum, int lsn) {
      modifiedBy = txnum;
      logSequenceNumber = lsn;
      contents.setInt(offset, val);
   }
   
   public void setString(int offset, String val, int txnum, int lsn) {
      modifiedBy = txnum;
      logSequenceNumber = lsn;
      contents.setString(offset, val);
   }
   
   public Block block() {
      return blk;
   }
   
   void flush() {
      if (modifiedBy >= 0) {
         SimpleDB.logMgr().flush(logSequenceNumber);
         contents.write(blk);
         modifiedBy = -1;
      }
   }
   
   void pin() {
      pins++;
   }
   
   void unpin() {
      pins--;
   }
   
   boolean isPinned() {
      return pins > 0;
   }
   
   boolean isModifiedBy(int txnum) {
      return txnum == modifiedBy;
   }
   
   void assignToBlock(Block b) {
      flush();
      blk = b;
      contents.read(blk);
      pins = 0;
   }

   void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      fmtr.format(contents);
      blk = contents.append(filename);
      pins = 0;
   }
}