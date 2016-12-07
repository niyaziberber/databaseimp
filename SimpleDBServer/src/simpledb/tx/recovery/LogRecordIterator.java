package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import java.util.Iterator;
import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

class LogRecordIterator implements Iterator<LogRecord> {
   private Iterator<BasicLogRecord> iter = SimpleDB.logMgr().iterator();
   
   public boolean hasNext() {
      return iter.hasNext();
   }
   
   public LogRecord next() {
      BasicLogRecord rec = iter.next();
      int op = rec.nextInt();
      switch (op) {
         case CHECKPOINT:
            return new CheckpointRecord(rec);
         case START:
            return new StartRecord(rec);
         case COMMIT:
            return new CommitRecord(rec);
         case ROLLBACK:
            return new RollbackRecord(rec);
         case SETINT:
            return new SetIntRecord(rec);
         case SETSTRING:
            return new SetStringRecord(rec);
        
         // Modified for HW4   
         case NQCKPT:
            return new NQCheckpointRecord(rec);
         default:
            return null;
      }
   } 
   
   public void remove() {
      throw new UnsupportedOperationException();
   }
}
