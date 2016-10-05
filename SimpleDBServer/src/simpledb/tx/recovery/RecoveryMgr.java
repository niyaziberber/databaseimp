package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import simpledb.file.Block;
import simpledb.buffer.Buffer;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */
public class RecoveryMgr {
   private int txnum;

   /**
    * Creates a recovery manager for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RecoveryMgr(int txnum) {
      this.txnum = txnum;
      new StartRecord(txnum).writeToLog();
   }

   /**
    * Writes a commit record to the log, and flushes it to disk.
    */
   public void commit() {
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CommitRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Writes a rollback record to the log, and flushes it to disk.
    */
   public void rollback() {
      doRollback();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new RollbackRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   public static void checkpoint() {
      int lsn = new NQCheckpointRecord(Transaction.getActiveTx()).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Recovers uncompleted transactions from the log,
    * then writes a quiescent checkpoint record to the log and flushes it.
    */
   public void recover() {
      doRecover();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CheckpointRecord().writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Writes a setint record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.getInt(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetIntRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Writes a setstring record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.getString(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetStringRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Rolls back the transaction.
    * The method iterates through the log records,
    * calling undo() for each log record it finds
    * for the transaction,
    * until it finds the transaction's START record.
    */
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

   /**
    * Does a complete database recovery.
    * The method iterates through the log records.
    * Whenever it finds a log record for an unfinished
    * transaction, it calls undo() on that record.
    * The method stops when it encounters a CHECKPOINT record
    * or the end of the log.
    * In case it encounters NQCHECKPOINT record, the method will
    * stop when the START, COMMIT, ROLLBACK record of first txNum in
    * the activeTx list is found in the log.
    * (Note the record only has to look at first txNum because
    * the tx numbers are in a chronological order, oldest to latest)
    */
   private void doRecover() {
      Collection<Integer> finishedTxs = new ArrayList<Integer>();
      Iterator<LogRecord> iter = new LogRecordIterator();
       Integer nqCkptEndpoint = -1;
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         System.out.println(rec);

          if ((Arrays.asList(START, COMMIT, ROLLBACK).contains(rec.op())) && nqCkptEndpoint == rec.txNumber())
              return;

         if (rec.op() == CHECKPOINT)
            return;
         if (rec.op() == NQCHECKPOINT) {
             // get the activeTx list from NQCheckpointRecord.
             // filter out finishedTxs from activeTx (since they are committed already)
             // keep recovering until the all the tx to look for are looked at.
                // keep going until START,COMMIT,ROLLBACK of txs[0]
             // undo any actions that are in not in finishedTxs.
             NQCheckpointRecord nq = (NQCheckpointRecord) rec;
             List<Integer> filteredTxs = nq.getActiveTx()
                     .stream()
                     .filter(p -> !finishedTxs.contains(p))
                     .collect(Collectors.toList());
             // everything has been committed. No need to continue.
             if (filteredTxs.size() == 0) {
                 return;
             } else {
                 nqCkptEndpoint = filteredTxs.get(0);
             }
         }
         if (rec.op() == COMMIT || rec.op() == ROLLBACK)
            finishedTxs.add(rec.txNumber());
         else if (!finishedTxs.contains(rec.txNumber()))
            rec.undo(txnum);
      }
   }

   /**
    * Determines whether a block comes from a temporary file or not.
    */
   private boolean isTempBlock(Block blk) {
      return blk.fileName().startsWith("temp");
   }
}
