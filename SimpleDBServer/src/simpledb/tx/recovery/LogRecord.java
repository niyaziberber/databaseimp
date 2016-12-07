package simpledb.tx.recovery;

import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

public interface LogRecord {
   
   static final int CHECKPOINT = 0, START = 1,
      COMMIT = 2, ROLLBACK  = 3,
      SETINT = 4, SETSTRING = 5,
      // Modified for HW4
      NQCKPT = 6;
   
   static final LogMgr logMgr = SimpleDB.logMgr();
   
   int writeToLog();
   int op();
   int txNumber();
   void undo(int txnum);
}