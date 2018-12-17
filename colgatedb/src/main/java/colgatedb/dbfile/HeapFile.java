package colgatedb.dbfile;

import colgatedb.*;
import colgatedb.page.*;
import colgatedb.transactions.Permissions;
import colgatedb.transactions.Transaction;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.nio.Buffer;
import java.util.NoSuchElementException;

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
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with SlottedPage. The format of SlottedPages is described in the javadocs
 * for SlottedPage.
 *
 * @see SlottedPage
 */
public class HeapFile implements DbFile {

    private final SlottedPageMaker pageMaker;   // this should be initialized in constructor

    private TupleDesc td;
    private int pageSize;
    private int tableid;
    private int numPages;
    private int currentPage;

    /**
     * Creates a heap file.
     * @param td the schema for records stored in this heapfile
     * @param pageSize the size in bytes of pages stored on disk (needed for PageMaker)
     * @param tableid the unique id for this table (needed to create appropriate page ids)
     * @param numPages size of this heapfile (i.e., number of pages already stored on disk)
     */
    public HeapFile(TupleDesc td, int pageSize, int tableid, int numPages) {
        this.td = td;
        this.pageSize = pageSize;
        this.tableid = tableid;
        this.numPages = numPages;
        pageMaker = new SlottedPageMaker(td, pageSize);

        if (!(numPages > 0)) {
            currentPage = -1;
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numPages;
    }

    @Override
    public int getId() {
        return tableid;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        AccessManager am = Database.getAccessManager();
        PageId pid = getFreePage(am, tid);
        SlottedPage p;
        synchronized (this) {
            if (pid == null) {
                pid = new SimplePageId(tableid, numPages);
                am.allocatePage(pid);
                numPages++;
            }
        }
        am.acquireLock(tid, pid, Permissions.READ_WRITE);
        p = (SlottedPage) am.pinPage(tid, pid, pageMaker);
        try {
            p.insertTuple(t);
        } catch (PageException e) {
            throw new DbException("[ERROR] " + e);
        }
        am.unpinPage(tid, p, true);
    }


    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        try {
            AccessManager am = Database.getAccessManager();
            SlottedPage p = getSlottedPage(tid, t.getRecordId().getPageId());
            am.acquireLock(tid, t.getRecordId().getPageId(),  Permissions.READ_WRITE);
            p.deleteTuple(t);
            am.unpinPage(tid, p, true);
        } catch (DbException e) {
            throw new DbException("[ERROR] the tuple cannot be deleted or it is not a member of the file");
        }
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * Finds the next page with a free slot
     * @param am the access manager
     * @return the PageId of the next free page or null if there is no page with a free slot
     */
    private PageId getFreePage(AccessManager am, TransactionId tid) throws TransactionAbortedException {
        boolean lockWasHeld = false;
        int initialCurrentPage = currentPage;
        if (currentPage == -1) {
            return null;
        }
        while (true) {
            SimplePageId pid = new SimplePageId(tableid, currentPage);
            SlottedPage p;
            if (am.holdsLock(tid, pid, Permissions.READ_WRITE) || am.holdsLock(tid, pid, Permissions.READ_ONLY)) {
                lockWasHeld = true;
            }
            if (!lockWasHeld) {
                am.acquireLock(tid, pid, Permissions.READ_ONLY);
            }
            p = (SlottedPage) am.pinPage(tid, pid, pageMaker);
            am.unpinPage(tid, p, false);
            if (!lockWasHeld) {
                am.releaseLock(tid, pid);
            }

            if (p.getNumEmptySlots() > 0) {
                return pid;
            }

            currentPage++;
            if (currentPage == numPages) {
                currentPage = 0;
            }
            if (currentPage == initialCurrentPage) {
                break;
            }
        }
        return null;
    }

    private SlottedPage getSlottedPage(TransactionId tid, PageId pid) {
        AccessManager am = Database.getAccessManager();
        return (SlottedPage)am.pinPage(tid, pid, pageMaker);
    }

    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        private boolean isOpen;
        private int currPage;
        private int currSlot;
        private PageId pid;
        private AccessManager am;
        private TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            am = Database.getAccessManager();
            this.tid = tid;
            currPage = 0;
            currSlot = 0;

        }

        @Override
        public void open() throws TransactionAbortedException {
            isOpen = true;
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            if (isOpen && currPage < numPages) {
                pid = new SimplePageId(tableid, currPage);
                SlottedPage p = (SlottedPage)am.pinPage(tid, pid, pageMaker);
                if (currSlot >= p.getNumSlots()) {
                    currSlot = 0;
                    currPage++;
                    am.unpinPage(tid, p, false);
                    return hasNext();
                } else if (p.isSlotEmpty(currSlot)) {
                    currSlot++;
                    am.unpinPage(tid, p, false);
                    return hasNext();
                } else {
                    am.unpinPage(tid, p, false);
                    return true;
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            SlottedPage p = (SlottedPage)am.pinPage(tid, pid, pageMaker);
            Tuple t = p.getTuple(currSlot);
            currSlot++;
            am.unpinPage(tid, p, false);
            return t;
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            currPage = 0;
            currSlot = 0;
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }

}
