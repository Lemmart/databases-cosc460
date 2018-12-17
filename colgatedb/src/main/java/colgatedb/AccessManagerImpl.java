package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.page.SimplePageId;
import colgatedb.transactions.*;

import javax.sound.midi.SysexMessage;
import java.util.*;

/**
 * ColgateDB
 *
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
public class AccessManagerImpl implements AccessManager {

    private boolean force = true;  // indicates whether force policy should be used
    private BufferManager bm;
    private LockManagerImpl lm;
    private Map<PageId, ArrayList<TransactionId>> pinnedPages;
    private Map<TransactionId, ArrayList<PageId>> tidsWithPages;

    /**
     * Initialize the AccessManager, which includes creating a new LockManager.
     * @param bm buffer manager through which all page requests should be made
     */
    public AccessManagerImpl(BufferManager bm) {
        this.bm = bm;
        lm = new LockManagerImpl();
        pinnedPages = new HashMap<>();
        tidsWithPages = new HashMap<>();
        bm.evictDirty(false);
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        lm.acquireLock(tid, pid, perm);
    }

    @Override
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        return lm.holdsLock(tid, pid, perm);
    }

    @Override
    public void releaseLock(TransactionId tid, PageId pid) {
        lm.releaseLock(tid, pid);
    }

    @Override
    public synchronized Page pinPage(TransactionId tid, PageId pid, PageMaker pageMaker) {
        if (!pinnedPages.containsKey(pid)) {
            pinnedPages.put(pid, new ArrayList<>());
            tidsWithPages.put(tid, new ArrayList<>());
        }
        pinnedPages.get(pid).add(tid);
        tidsWithPages.get(tid).add(pid);
        return bm.pinPage(pid, pageMaker);
    }

    @Override
    public synchronized void unpinPage(TransactionId tid, Page page, boolean isDirty) {
        pinnedPages.get(page.getId()).remove(tid);
        bm.unpinPage(page.getId(), isDirty);
    }

    @Override
    public synchronized void allocatePage(PageId pid) {
        bm.allocatePage(pid);
    }

    @Override
    public synchronized void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    @Override
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        ArrayList<PageId> pids = tidsWithPages.get(tid);
        System.out.println("AMI: size: " + pids.size());
        for (PageId pid : pids) {
            System.out.println("AMI: pid is " + pid.toString());
            if (bm.isDirty(pid)) {
                System.out.println("AMI: pid is dirty. commit && force: " + (commit && force));
                if (commit && force) {
                    System.out.println("AMI: ...flushing");
                    bm.flushPage(pid);
                } else {
                    bm.unpinPage(pid, true);
                    bm.discardPage(pid);
                }
            } else {
                bm.unpinPage(pid, false);
            }
            System.out.println("AMI: tidsWithPages.size()=" + tidsWithPages.size() + " --- pinnedPages.size()="+pinnedPages.size());
            tidsWithPages.get(tid).remove(pid);
            pinnedPages.get(pid).remove(tid);
            lm.releaseLock(tid, pid);
        }
    }

    @Override
    public void setForce(boolean force) {
        // you do NOT need to implement this for lab10.  this will be changed in a later lab.
//         throw new UnsupportedOperationException("implement me!");
        this.force = force;
    }
}
