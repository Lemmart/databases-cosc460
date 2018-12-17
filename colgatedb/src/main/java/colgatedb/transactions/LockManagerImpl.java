package colgatedb.transactions;

import colgatedb.page.PageId;

import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */
public class LockManagerImpl implements LockManager {

    private ArrayList<LockTableEntry> lockTableEntries;

    public LockManagerImpl() {
        lockTableEntries = new ArrayList<>();
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        System.out.println("LMI: added holdsLock check to acquireLock method - please remove me if not applicable!");
        if (holdsLock(tid, pid, perm)) {
            return;
        }
        LockTableEntry e = getEntry(pid);
        synchronized (this) {
            e.addEntry(tid, perm);
        }

//        busy wait for lock
        while (true) {
            boolean exceededTicks;
            boolean holds;
            synchronized (this) {
                exceededTicks = e.processLock(tid);
                holds = holdsLock(tid, pid, perm);
//                deadlock detected!
                if (exceededTicks) {
                    e.cleanUpDeadlock(tid, perm);
                    throw new TransactionAbortedException();
                }
//            successfully acquired lock!
                else if (holds) {
                    break;
                }
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException x) {}
        }
    }

    @Override
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        for (LockTableEntry e : lockTableEntries) {
            if (e.holdsLock(tid,pid, perm)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void releaseLock(TransactionId tid, PageId pid) {
        for (LockTableEntry e : lockTableEntries) {
            if (e.equalsPid(pid) && e.getTids().contains(tid)) {
                e.releaseLock(tid);
                return;
            }
        }
        throw new LockManagerException("[ERROR] Failed to release lock. Transaction ID " + tid.toString() + " not found.");
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        ArrayList<PageId> pids = new ArrayList<>();
        for (LockTableEntry e : lockTableEntries) {
            if (e.getTids().contains(tid)) {
                pids.add(e.getPid());
            }
        }
        return pids;
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        for (LockTableEntry e : lockTableEntries) {
            if (e.equalsPid(pid)) {
                return e.getTids();
            }
        }
        throw new NullPointerException();
    }

    private synchronized LockTableEntry getEntry(PageId pid) {
        for (LockTableEntry e : lockTableEntries) {
            if (e.equalsPid(pid)) {
                return e;
            }
        }
        LockTableEntry p = new LockTableEntry(pid);
        lockTableEntries.add(p);
        return p;
    }
}
