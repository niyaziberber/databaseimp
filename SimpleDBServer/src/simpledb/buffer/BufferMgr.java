package simpledb.buffer;

import simpledb.file.*;

public class BufferMgr {
   private static final long MAX_TIME = 10000; // 10 seconds
   private BasicBufferMgr bufferMgr;
   
   public BufferMgr(int numbuffers) {
      bufferMgr = new BasicBufferMgr(numbuffers);
   }
   
   // new method for HW3
   public String toString() {
	   return bufferMgr.toString();
   }
   
   public synchronized Buffer pin(Block blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = bufferMgr.pin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = bufferMgr.pin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }
   
   public synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = bufferMgr.pinNew(filename, fmtr);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = bufferMgr.pinNew(filename, fmtr);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }

   public synchronized void unpin(Buffer buff) {
      bufferMgr.unpin(buff);
      if (!buff.isPinned())
         notifyAll();
   }

   public void flushAll(int txnum) {
      bufferMgr.flushAll(txnum);
   }

   public int available() {
      return bufferMgr.available();
   }
   
   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }
}
