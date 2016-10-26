package simpledb.tx.concurrency;

import simpledb.file.Block;

import javax.xml.bind.SchemaOutputResolver;
import java.util.*;

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then that transaction is placed on a wait list.
 * There is only one wait list for all blocks.
 * When the last lock on a block is unlocked, then all transactions
 * are removed from the wait list and rescheduled.
 * If one of those transactions discovers that the lock it is waiting for
 * is still locked, it will place itself back on the wait list.
 * @author Edward Sciore
 */
class LockTable {
   private static final long MAX_TIME = 10000; // 10 seconds
   
   private static Map<Block,List<Integer>> locks = new HashMap<>();
   
   /**
    * Grants an SLock on the specified block.
    * If an XLock exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the lock is released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   public synchronized void sLock(Block blk, int txnum) {
      try {
         while (hasXlock(blk)) {
            if (!olderTxExists(blk, txnum))
                wait(MAX_TIME);
             else
                 throw new LockAbortException();
         }
         if (hasXlock(blk))
             throw new LockAbortException();
          addLock(blk, txnum);
      }
      catch(InterruptedException e) {
         throw new LockAbortException();
      }
   }

   private void addLock(Block blk, int txnum) {
       if (!locks.containsKey(blk)) {
           ArrayList<Integer> lst = new ArrayList<>();
           locks.put(blk, lst);
       }
       locks.get(blk).add(txnum);
   }
   
   /**
    * Grants an XLock on the specified block.
    * If a lock of any type exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the locks are released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   synchronized void xLock(Block blk, int txnum) {
      try {
         while (hasOtherLocks(blk)) {
             if (!olderTxExists(blk, txnum))
                 wait(MAX_TIME);
             else
                 throw new LockAbortException();
         }
         if (hasOtherLocks(blk))
            throw new LockAbortException();
         addLock(blk, -1*txnum);
      }
      catch(InterruptedException e) {
         throw new LockAbortException();
      }
   }
   
   /**
    * Releases a lock on the specified block.
    * If this lock is the last lock on that block,
    * then the waiting transactions are notified.
    * @param blk a reference to the disk block
    */
   synchronized void unlock(Block blk, int txnum) {
       if(locks.get(blk).size() > 1 && !locks.get(blk).contains(0-txnum)) {
           locks.get(blk).remove((Integer) txnum);
       } else {
           locks.remove(blk);
           notifyAll();
       }
   }

   private boolean olderTxExists(Block blk, int txnum) {
       for (Integer existingTxnum: locks.get(blk)) {
           if (Math.abs(existingTxnum) < txnum)
               return true;
       }
       return false;
   }
   
   private boolean hasXlock(Block blk) {
       if (locks.get(blk) == null)
           return false;
       for (Integer existingTxnum: locks.get(blk)) {
           if (existingTxnum < 0)
           return true;
       }
        return false;
   }

    private boolean hasOtherLocks(Block blk) {
        return locks.get(blk) != null && locks.get(blk).size() > 1;
    }


}
