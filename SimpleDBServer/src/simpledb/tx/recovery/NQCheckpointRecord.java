package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;
import simpledb.tx.Transaction;

import java.util.*;

/**
 * The NQCHECKPOINT log record.
 * @author Edward Sciore
 */
class NQCheckpointRecord implements LogRecord {
    private List<Integer> activeTx = new ArrayList<>();
    /**
     * Creates a nonquiescent checkpoint record.
     */
    public NQCheckpointRecord(List<Transaction> tx) {
        for (Transaction t : tx) {
            activeTx.add(Integer.valueOf(t.toString()));
        }
    }

    /**
     * Creates a log record by reading string encoded integer list
     * from the basic log record.
     * @param rec the basic log record
     */
    public NQCheckpointRecord(BasicLogRecord rec) { this.activeTx = listfyString(rec.nextString());}

    /**
     * Writes a checkpoint record to the log.
     * This log record contains the NQCHECKPOINT operator,
     * and the set of active transactions encoded in String.
     * @return the LSN of the last log value
     */
    public int writeToLog() {
        String writableList = stringfyList(activeTx);
        Object[] rec = new Object[] {NQCHECKPOINT, writableList};
        System.out.println("NQ CHECKPOINT: Transactions " + writableList + " are still active.");
        return logMgr.append(rec);
    }

    /**
     * encodes a list into a string with " " delimiter.
     * Used for logging purpose as it only takes String or Integer.
     * @param tx
     * @return
     */
    private String stringfyList(List<Integer> tx) {
        String s = "";
        for (int i = 0; i < tx.size()-1; i++) {
            s += tx.get(i)+" ";
        }
        s+=(tx.get(tx.size()-1));
        return s;
    }

    /**
     * decodes string delimited by space (" ") into an integer list.
     * @param s encoded String
     * @return decoded List
     */
    private List<Integer> listfyString(String s) {
        List<String> ls =  Arrays.asList(s.split(" "));
        List<Integer> li = new ArrayList<Integer>();
        for (String ss : ls) {
            li.add(Integer.valueOf(ss));
        }
        return li;
    }

    public int op() {
        return NQCHECKPOINT;
    }

    /**
     * Checkpoint records have no associated transaction,
     * and so the method returns a "dummy", negative txid.
     */
    public int txNumber() {
        return -1; // dummy value
    }

    /**
     * Does nothing, because a checkpoint record
     * contains no undo information.
     */
    public void undo(int txnum) {}

    public String toString() {
        return "<NQCHECKPOINT " + stringfyList(activeTx) + ">";
    }

    public List<Integer> getActiveTx() {
        return activeTx;
    }
}
