package colgatedb.transactions;

import colgatedb.page.PageId;

import javax.sound.midi.SysexMessage;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
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

/**
 * Represents the state associated with the lock on a particular page.
 */
public class LockTableEntry {

    private Permissions lockType;             // null if no one currently has a lock
    private Set<TransactionId> lockHolders;   // a set of txns currently holding a lock on this page
    private ArrayList<LockRequest> requests;       // a queue of outstanding requests
    private PageId pid;
    private int tickCount;

    public LockTableEntry() {
        lockType = null;
        lockHolders = new HashSet<>();
        requests = new ArrayList<>();
        pid = null;
        tickCount = 0;
    }

    public LockTableEntry(PageId pid) {
        this();
        this.pid = pid;
    }

    public void addEntry(TransactionId tid, Permissions perm) {
        if (holdsLock(tid, this.pid, perm)) {
            return;
        }
        if (lockHolders.contains(tid) && perm.permLevel == 1) {
            requests.add(0, new LockRequest(tid, perm));
        } else {
            requests.add(new LockRequest(tid, perm));
        }
    }

    public boolean processLock(TransactionId tid) {
        LockRequest r = requests.get(0);
        if (tid.equals(r.tid)) {
//            allow first thread to grab lock
            if (lockType == null || lockHolders.isEmpty() || (lockType.permLevel == r.perm.permLevel && lockType.permLevel == 0)) {
                lockType = r.perm;
                lockHolders.add(requests.remove(0).tid);
                tickCount = 0;
            }
//                upgrade currently held lock
            else if (lockHolders.size() == 1 && lockHolders.contains(r.tid) && (lockType.permLevel > 0 || r.perm.permLevel > 0)) {
                lockType = Permissions.READ_WRITE;
                lockHolders.add(requests.remove(0).tid);
                tickCount = 0;
            }
            tickCount++;
        }
        return tickCount > ThreadLocalRandom.current().nextInt(1000,1500);
    }

    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        return lockType != null && lockHolders.contains(tid) && this.pid.equals(pid) && perm.permLevel <= lockType.permLevel;
    }

    public void cleanUpDeadlock(TransactionId tid, Permissions perm) {
        if (requests.size() > 0 && requests.get(0).tid.equals(tid) && requests.get(0).perm.permLevel == perm.permLevel) {
            requests.remove(0);
        }
    }

    public boolean releaseLock(TransactionId tid) {
        if (!lockHolders.contains(tid)) return false;

        lockHolders.remove(tid);
        if (lockHolders.isEmpty()) {
            lockType = null;
        }
        return true;
    }

    public boolean contains(PageId pid, TransactionId tid, Permissions perm) {
        for (LockRequest r : requests) {
            if (r.tid.equals(tid) && lockType.equals(perm)) {
                return this.pid.equals(pid);
            }
        }
        return this.pid.equals(pid) && lockHolders.contains(tid);
    }

    public boolean equalsPid(PageId pid) {
        return this.pid.equals(pid);
    }

    public List<TransactionId> getTids() {
        return new ArrayList<>(lockHolders);
    }

    public PageId getPid() {
        return pid;
    }

    /**
     * A class representing a single lock request.  Simply tracks the txn and the desired lock type.
     * Feel free to use this, modify it, or not use it at all.
     */
    private class LockRequest {
        public final TransactionId tid;
        public final Permissions perm;

        public LockRequest(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        public boolean equals(Object o) {
            if (!(o instanceof LockRequest)) {
                return false;
            }
            LockRequest otherLockRequest = (LockRequest) o;
            return tid.equals(otherLockRequest.tid) && perm.equals(otherLockRequest.perm);
        }

        public String toString() {
            return "Request[" + tid + "," + perm + "]";
        }
    }
}
