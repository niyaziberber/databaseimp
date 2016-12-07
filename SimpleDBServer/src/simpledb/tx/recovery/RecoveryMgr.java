package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import simpledb.file.Block;
import simpledb.buffer.Buffer;
import simpledb.server.SimpleDB;
import java.util.*;

public class RecoveryMgr {
   private int txnum;
   
   public RecoveryMgr(int txnum) {
      this.txnum = txnum;
      new StartRecord(txnum).writeToLog();
   }
   
   public void commit() {
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CommitRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }
   
   public void rollback() {
      SimpleDB.bufferMgr().flushAll(txnum);
      doRollback();
      int lsn = new RollbackRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }
   
   public void recover() {
      doRecover();
      int lsn = new CheckpointRecord().writeToLog();
      SimpleDB.logMgr().flush(lsn);
      
   }
   
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.getInt(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetIntRecord(txnum, blk, offset, oldval).writeToLog();
   }
   
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.getString(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetStringRecord(txnum, blk, offset, oldval).writeToLog();
   }
   
   private void doRollback() {
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(txnum);
         }
      }
   }

   // Modified for HW4
   private void doRecover() {
      Collection<Integer> committedTxs = new ArrayList<Integer>();
      Collection<Integer> remainingTxs = null;   // added for hw4
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         System.out.println(rec);  // added for hw4
         if (rec.op() == CHECKPOINT)
            return;
         else if (rec.op() == NQCKPT) { // added for hw4
            NQCheckpointRecord nqrec = (NQCheckpointRecord) rec;
            remainingTxs = nqrec.txs();
            for (Integer i : committedTxs)
               remainingTxs.remove(i);
System.out.print("NQ: Remaining txs= ");
for (int i : remainingTxs)
	System.out.print(i + " ");
System.out.println();
         }
         if (rec.op() == COMMIT)
            committedTxs.add(rec.txNumber());
         else if (rec.op() == START && remainingTxs != null) {  // for hw4
            remainingTxs.remove(new Integer(rec.txNumber()));
            if (remainingTxs.isEmpty())
               break;
         }
         else if (!committedTxs.contains(rec.txNumber())) 
            rec.undo(txnum);
      }
   }
   
   // New method for HW4
   public static void checkpoint(Collection<Integer> txs) {
      System.out.print("NQ CHECKPOINT: Transactions ");
      for (Integer txid : txs)
    	  System.out.print(txid + " ");
      System.out.println(" are still active");
      new NQCheckpointRecord(txs).writeToLog();
   }

   private boolean isTempBlock(Block blk) {
      return blk.fileName().startsWith("temp");
   }
}
