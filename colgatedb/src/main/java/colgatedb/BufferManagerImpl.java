package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.util.ArrayList;

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
        int pidIdx = getFrameIndex(pid);
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
        int pidIdx = getFrameIndex(pid);
        if (pidIdx == -1 || bufferPool.get(pidIdx).pinCount == 0) {
            throw new BufferManagerException("[ERROR] Unable to unpin page at index " + pidIdx + ".");
        }
        Frame f = bufferPool.get(pidIdx);
        f.pinCount--;
        if (isDirty){
            f.isDirty = isDirty;
        }
    }

    @Override
    public synchronized void flushPage(PageId pid) {
        int pidIdx = getFrameIndex(pid);
        if (pidIdx != -1 && bufferPool.get(pidIdx).isDirty && bufferPool.get(pidIdx).pinCount == 0) {
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
        int pidIdx = getFrameIndex(pid);
        return pidIdx > -1 && bufferPool.get(pidIdx).isDirty;
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        return (getFrameIndex(pid) > -1);
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        int pidIdx = getFrameIndex(pid);
        if (pidIdx > -1) {
            return bufferPool.get(pidIdx).page;
        }
        throw new BufferManagerException("[ERROR] Failed to retrieve page. Page ID not found in cache.");
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        int pidIdx = getFrameIndex(pid);
        if (pidIdx > -1) {
            bufferPool.set(pidIdx, new Frame(null));
        }
    }

    private int getFrameIndex(PageId pid) {
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

    /**
     * Implements a clock replacement scheme using the global tracking variable evictionIdx. It
     * iterates frame by frame through the buffer pool, pulling one frame into memory at a time.
     * This implementation is highly efficient (potenially O(1) )in that successive calls will
     * cause the method to immediately jump to the evictionIdx index in the buffer pool, removing
     * a potentially long search through frames that have not changed/emptied since the previous
     * search. Thus in the worst case, it operates in O(n) time whereas in the best case (multiple
     * free slots on the same page), it operates in O(1) time. Rather than
     */
    private int evictPage() {
        int loops = 0;
        int idx = -1;
        while (idx == -1) {
            Frame f = bufferPool.get(evictionIdx);
            if (f.pinCount == 0) {
                if (f.recentlyUsed) {
                    f.recentlyUsed = false;
                } else if (!f.isDirty) {
                     idx = evictionIdx;
                } else if (allowEvictDirty) {
                    flushPage(f.page.getId());
                    idx = evictionIdx;
                }
            }

            evictionIdx++;
            if (loops >= 2) {
                throw new BufferManagerException("[ERROR] No available frames for eviction!");
            }
            loops = resetEvictionIdx(loops);
        }
        return idx;
    }

    private int resetEvictionIdx(int loops) {
        if (evictionIdx == bufferPool.size()) {
            evictionIdx = 0;
            return (++loops);
        }
        return loops;
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