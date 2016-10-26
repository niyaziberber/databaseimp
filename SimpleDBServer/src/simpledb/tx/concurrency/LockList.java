package simpledb.tx.concurrency;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Bryan Lee
 * Date: 10/18/16
 */
public class LockList {
    class Lock {
        boolean isLocked = false;
        int index = -1;
    }

    private List<Integer> lockList = new ArrayList<>();
    private Lock xLock = new Lock();
    private Lock sLock = new Lock();

    /**
     * adds a lock to the list. If lock is negative, it implies xLock.
     * @param txnum
     */
    public void add(Integer txnum) {
        lockList.add(txnum);
        if (txnum < 0) {
            xLock.isLocked = true;
            xLock.index = lockList.get(txnum);
        }
        else {
            sLock.isLocked = true;
            sLock.index = lockList.get(txnum);
        }
    }

    public void remove(Integer txnum) {
        lockList.remove(txnum);


    }

    public boolean hasXLock() {
        return xLock.isLocked;
    }

    public boolean hasSLock() {
        return sLock.isLocked;
    }


}
