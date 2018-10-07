package colgatedb.dbfile;

import colgatedb.BufferManager;
import colgatedb.Database;
import colgatedb.page.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

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
        BufferManager bm = Database.getBufferManager();
        PageId pid = getFreePage(bm);
        if (pid == null) {
            SimplePageId newPid = new SimplePageId(tableid, currentPage);
            bm.allocatePage(newPid);
            bm.pinPage(newPid, pageMaker);
        } else {
            bm.pinPage(pid, pageMaker);
        }
    }


    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    private PageId getFreePage(BufferManager bm) {
        boolean isFree = false;
        int initialCurrentPage = currentPage;
        while (!isFree) {
            SimplePageId pid = new SimplePageId(tableid, currentPage);
            SlottedPage p = (SlottedPage)bm.getPage(pid);

            if (p.getNumEmptySlots() > 0) {
                int slotno = 0;
                while (p.isSlotEmpty(slotno)) {

                    slotno++;
                }
            }


            currentPage++;
            if (initialCurrentPage == currentPage) {
                break;
            }
        }
    }

    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {

        public HeapFileIterator(TransactionId tid) {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public void open() throws TransactionAbortedException {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("implement me!");
        }
    }

}
