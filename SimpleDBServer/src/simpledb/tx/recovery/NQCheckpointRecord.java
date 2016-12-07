package simpledb.tx.recovery;

import simpledb.log.*;
import java.util.*;

// New for HW4

public class NQCheckpointRecord implements LogRecord {
   private Collection<Integer> txs = new ArrayList<Integer>();
   
   public int writeToLog() {
      int size = txs.size();
      Object[] rec = new Object[size+2];
      rec[0] = NQCKPT;
      rec[1] = size;
      int count = 0;
      for (Integer txid : txs) {
         rec[count+2] = txid;
         count++;
      }
      return logMgr.append(rec);
   }
   
   public NQCheckpointRecord(Collection<Integer> txs) {
      this.txs = txs;
   }
   
   public NQCheckpointRecord(BasicLogRecord rec) {
      int size = rec.nextInt();
      for (int i=0; i<size; i++)
         txs.add(new Integer(rec.nextInt()));
   }
   
   public Collection<Integer> txs() {
      return txs;
   }
   
   public int op() {
      return LogRecord.NQCKPT;
   }
   
   public int txNumber() {
      return -1; // dummy value
   }
   
   public void undo(int txnum) {}
   
   public String toString() {
      String result = "<NQCHECKPOINT ";
      for (Integer i : txs)
         result += i + " ";
      return result + ">";
   }
}
