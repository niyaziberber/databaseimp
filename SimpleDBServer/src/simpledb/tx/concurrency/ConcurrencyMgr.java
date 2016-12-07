package simpledb.tx.concurrency;

import simpledb.file.Block;
import java.util.*;

/**
 * The concurrency manager for the transaction.
 * @author Edward Sciore
 */
public class ConcurrencyMgr {

	/**
	 * The global lock table.  This variable is static because all transactions
	 * share the same table.
	 */
	private static LockTable locktbl = new LockTable();
	private Map<Block,String> locks  = new HashMap<Block,String>();
	private int txnum;  // added for hw5

	public ConcurrencyMgr(int txnum) {
		this.txnum = txnum;
	}

	/**
	 * Obtains an SLock on the block, if necessary.
	 * @param blk a reference to the disk block
	 */
	public void sLock(Block blk) {
		if (locks.get(blk) == null) {
			locktbl.sLock(blk, txnum);  // modified for hw5
			locks.put(blk, "S");
		}
	}

	/**
	 * Obtains an XLock on the block, if necessary.
	 * An XLock is obtained by first getting an SLock
	 * and then upgrading to an XLock.
	 * @param blk a refrence to the disk block
	 */
	public void xLock(Block blk) {
		if (!hasXLock(blk)) {
			sLock(blk);
			locktbl.xLock(blk, txnum);  // modified for hw5
			locks.put(blk, "X");
		}
	}

	/**
	 * Releases all locks.
	 */
	public void release() {
		for (Block blk : locks.keySet())
			locktbl.unlock(blk, txnum);  // modified for hw5
		locks.clear();
	}

	private boolean hasXLock(Block blk) {
		String locktype = locks.get(blk);
		return locktype != null && locktype.equals("X");
	}
}
