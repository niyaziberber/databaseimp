package simpledb.tx.concurrency;

import simpledb.file.Block;
import java.util.*;

public class LockTable {
	// For HW5: Delete the MAX_TIME constant
	
	// New for HW 5: the value of the map is now a list of tx ids.
	private Map<Block,List<Integer>> locks = new HashMap<Block,List<Integer>>();

	// All methods have been changed substantially.
	
	public synchronized void sLock(Block blk, int txnum) {
		try {
			while (hasXlock(blk)) {
				checkForAbort(blk, txnum);
				wait();
			}
			List<Integer> txs = getLockVal(blk);
			txs.add(txnum);
			locks.put(blk, txs);
		}
		catch(InterruptedException e) {
			throw new LockAbortException();
		}
	}

	public synchronized void xLock(Block blk, int txnum) {
		try {
			while (hasOtherSLocks(blk)) {
				checkForAbort(blk, txnum);
				wait();
			}
			List<Integer> txs = new ArrayList<Integer>();
			txs.add(-txnum);
			locks.put(blk, txs);
		}
		catch(InterruptedException e) {
			throw new LockAbortException();
		}
	}

	public synchronized void unlock(Block blk, int txnum) {
		List<Integer> txs = getLockVal(blk);
		if (txs.size() > 1) {
			txs.remove(new Integer(txnum));
			locks.put(blk, txs);
		}
		else {
			locks.remove(blk);
			notifyAll();
		}
	}

	private boolean hasXlock(Block blk) {
		List<Integer> txs = getLockVal(blk);
		return (txs.size() > 0 && txs.get(0) < 0);
	}

	private boolean hasOtherSLocks(Block blk) {
		List<Integer> txs = getLockVal(blk);
		return txs.size() > 1;
	}

	private void checkForAbort(Block blk, int txnum) {
		List<Integer> txs = getLockVal(blk);
		for (int n : txs)
			if (Math.abs(n) < txnum)
				throw new LockAbortException();
	}

	private List<Integer> getLockVal(Block blk) {
		List<Integer> txs = locks.get(blk);
		return (txs == null) ? new ArrayList<Integer>() : txs;
	}
}
