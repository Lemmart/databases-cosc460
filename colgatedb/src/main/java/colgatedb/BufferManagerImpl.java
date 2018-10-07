package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.util.ArrayList;

import java.util.concurrent.TimeUnit;

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
public class BufferManagerImpl implements BufferManager {

    private boolean allowEvictDirty = false;  // a flag indicating whether a dirty page is candidate for eviction
    private DiskManager dm;
    private ArrayList<Frame> bufferPool;
    private int evictionIdx;

    /**
     * Construct a new buffer manager.
     * @param numPages maximum size of the buffer pool
     * @param dm the disk manager to call to read/write pages
     */
    public BufferManagerImpl(int numPages, DiskManager dm) {
        this.dm = dm;
        bufferPool = new ArrayList<>();
        for (int i = 0; i < numPages; i++) {
            bufferPool.add(new Frame(null));
        }
        evictionIdx = 0;
    }

    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {
        int pidIdx = getPageIndex(pid);
        Frame f;
        Page p;
        if (pidIdx != -1) {
            f = bufferPool.get(pidIdx);
            f.pinCount++;
            p = f.page;
            f.recentlyUsed = true;
        } else {
            p = dm.readPage(pid, pageMaker);
            int emptyIdx = getNextEmptyIndex();
            if (emptyIdx > -1) {
                bufferPool.set(emptyIdx, new Frame(p));
            } else {
                bufferPool.set(evictPage(), new Frame(p));
            }
        }
        return p;
    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {
        int pidIdx = getPageIndex(pid);
        if (pidIdx == -1 || bufferPool.get(pidIdx).pinCount == 0) {
            throw new BufferManagerException("[ERROR] Unable to unpin page at index " + pidIdx + ".");
        }
        Frame f = bufferPool.get(pidIdx);
        f.pinCount--;
        if (!f.isDirty) {
            f.isDirty = isDirty;
        }
    }

    @Override
    public synchronized void flushPage(PageId pid) {
        int pidIdx = getPageIndex(pid);
        if (pidIdx != -1 && bufferPool.get(pidIdx).isDirty) {
            dm.writePage(bufferPool.get(pidIdx).page);
            bufferPool.set(pidIdx, new Frame(null));
        }
    }

    @Override
    public synchronized void flushAllPages() {
        for (Frame f: bufferPool) {
            if (f.page != null) {
                flushPage(f.page.getId());
            }
        }
    }

    @Override
    public synchronized void evictDirty(boolean allowEvictDirty) {
        this.allowEvictDirty = allowEvictDirty;
    }

    @Override
    public synchronized void allocatePage(PageId pid) {
        dm.allocatePage(pid);
    }

    @Override
    public synchronized boolean isDirty(PageId pid) {
        int pidIdx = getPageIndex(pid);
        if (pidIdx > -1 && bufferPool.get(pidIdx).isDirty) {
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        return (getPageIndex(pid) > -1);
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        int pidIdx = getPageIndex(pid);
        if (pidIdx > -1) {
            return bufferPool.get(pidIdx).page;
        }
        throw new BufferManagerException("[ERROR] Failed to retrieve page. Page ID not found in cache.");
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        int pidIdx = getPageIndex(pid);
        if (pidIdx > -1) {
            bufferPool.set(pidIdx, new Frame(null));
        }
    }

    private int getPageIndex(PageId pid) {
        int idx = 0;
        for (Frame f : bufferPool) {
            if (f.page != null && f.page.getId().equals(pid)) {
                return idx;
            }
            idx++;
        }
        return -1;
    }

    private int getNextEmptyIndex() {
        int idx = 0;
        for (Frame f : bufferPool) {
            if (f.page == null) {
                return idx;
            }
            idx++;
        }
        return -1;
    }

    private int evictPage() {
        int initialEvictionIdx = evictionIdx;
        while (evictionIdx < bufferPool.size()) {
            Frame f = bufferPool.get(evictionIdx);
            if (allowEvictDirty && !f.recentlyUsed && f.pinCount == 0) {
                evictionIdx++;
                resetEvictionIdx();
                break;
            } else if (!allowEvictDirty && !f.isDirty && !f.recentlyUsed && f.pinCount == 0) {
                evictionIdx++;
                resetEvictionIdx();
                break;
            }

            bufferPool.get(evictionIdx).recentlyUsed = false;
            evictionIdx++;

            resetEvictionIdx();
            if (initialEvictionIdx == evictionIdx) {
                throw new BufferManagerException("[ERROR] No available frames for eviction!");
            }
        }
        if (bufferPool.get(evictionIdx).isDirty) {
            flushPage(bufferPool.get(evictionIdx).page.getId());
        }
        return evictionIdx;
    }

    private void resetEvictionIdx() {
        if (evictionIdx == bufferPool.size()) {
            evictionIdx = 0;
        }
    }


    /**
     * A frame holds one page and maintains state about that page.  You are encouraged to use this
     * in your design of a BufferManager.  You may also make any warranted modifications.
     */
    private class Frame {
        private Page page;
        private int pinCount;
        public boolean isDirty;
        private boolean recentlyUsed;

        public Frame(Page page) {
            this.page = page;
            this.pinCount = 1;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
            this.recentlyUsed = false;
        }
    }

}