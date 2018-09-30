package colgatedb.page;

import colgatedb.tuple.Field;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.*;
import java.util.*;

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
 * SlottedPageFormatter is responsible for translating a SlottedPage to and from a
 * byte array representation.
 * <p>
 * A SlottedPage has an array of slots, each of which can hold one tuple and all tuples have the same exact size,
 * that is determined by the TupleDesc for this page.
 * <p>
 * The page format has three components:
 * (a) header
 * (b) payload
 * (c) zeroed out excess bytes
 * The header is a bitmap, with one bit per tuple slot. If the bit corresponding to a particular slot is 1, it
 * indicates that the slot is occupied; if it is 0, the slot is considered empty.
 * <p>
 * The layout of the header requires some explanation.  The first byte of the header represents slots 0..7, the second
 * byte is slots 8..15, and so on.  However, within a byte, the least significant bit of represents the lowest slot
 * value. Thus, suppose the first byte looked like this:
 * bits:  10010110
 * this indicates that slots 1, 2, 4, and 7 are occupied and slots 0, 3, 5, and 6 are empty.  In other words, the bits
 * for the slots are arranged according to following pattern:
 *
 * 7,6,5,4,3,2,1,0  15,14,13,12,11,10,9,8  23,22,21,20,19,18,17,16  and so on.
 *
 * <p>
 * The payload is the data itself.  The tuples of the page are written out in slot order from slot 0 to slot N-1 where
 * N is the number of slots on the page.  If the slot is occupied, the bytes for that slot consist of the data for each
 * field in th tuple, written out in order.  Let k be the number of bytes required to store a tuple.  If the slot is
 * empty slot, then k bytes of zeros are written out.
 * <p>
 * After the last slot is written, there may be excess bytes.  These are just zeroed out.
 */
public class SlottedPageFormatter {

    /**
     * The tuple capacity is computed as follows:
     * - Each tuple has a tuple size (determined by the TupleDesc), which is measured in bytes.
     * - There are 8 bits in a byte.
     * - Additionally, each tuple requires 1 bit in header.
     * - Thus, given the pageSize (measured in bytes) we can store at most.
     *     floor((pageSize *8) / (tuple size * 8 + 1))
     *   tuples on a page.
     * @return number of tuples that this page can hold
     */
    public static int computePageCapacity(int pageSize, TupleDesc td) {
        int capacity = (int)Math.floor((pageSize * 8) / (td.getSize() * 8 + 1));
        return capacity;
    }

    /**
     * The size of the header is the number of bytes needed to store the header given that
     * each slot requires one bit.  This is equal to ceiling( numSlots / 8 ).
     *
     * @param numSlots
     * @return the size of the header in bytes.
     */
    public static int getHeaderSize(int numSlots) {
        return (int) Math.ceil(numSlots / 8.0);
    }

    /**
     * Write out the page to bytes.  See the javadoc at the top of file for byte format description.
     * @param page the page to write
     * @param td the TupleDesc that describes the tuples on the page
     * @param pageSize the size of the page
     * @return
     */
    public static byte[] pageToBytes(SlottedPage page, TupleDesc td, int pageSize) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(pageSize);
            DataOutputStream dos = new DataOutputStream(baos);

            writeHeader(dos, page);
            writePayload(dos, page, td, pageSize);

            return baos.toByteArray();
    }

    private static void writeHeader(DataOutputStream dos, SlottedPage page) {
        int numSlots = page.getNumSlots();
        int headerSize = getHeaderSize(numSlots); // number of bytes in header

        byte[] headerBytes = new byte[headerSize];

        for (int i = 0; i < headerSize*8; i++) {
            int byteIdx = i/8;
            if (page.isSlotUsed(i)) {
                byte b = (byte)(Math.pow(2,(double)(i%8)));
                headerBytes[byteIdx] = (byte)(headerBytes[byteIdx] | b);
            }
        }
        try {
            dos.write(headerBytes);
        } catch (Exception e) {
            throw new PageException(e);
        }
    }


    private static void writePayload(DataOutputStream dos, SlottedPage page, TupleDesc td, int pageSize) {
        int pageCapacity = computePageCapacity(pageSize, td);
        try {
            for (int i = 0; i < pageCapacity; i++) {
                if (page.isSlotUsed(i)) {
                    Iterator<Field> fIterator = page.getTuple(i).fields();
                    while (fIterator.hasNext()) {
                        fIterator.next().serialize(dos);
                    }
                } else {
                    int tupSize = td.getSize();
                    byte[] emptyTups = new byte[tupSize];
                    dos.write(emptyTups);
                }
            }
            while (dos.size() < pageSize) {
                dos.write(0);
            }
        } catch (Exception e) {
            throw new PageException(e);
        }
    }

    /**
     * Populate the empty page with data that is read from the given bytes.  See the javadoc at the top of file
     * for byte format description.
     * @param bytes bytes representing page data
     * @param emptyPage an initially emptyPage to be populated
     * @param td the TupleDesc of tuples on this page
     */
    public static void bytesToPage(byte[] bytes, SlottedPage emptyPage, TupleDesc td) {

        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));

            ArrayList<Integer> header = readHeader(dis, emptyPage);
            readPayload(header, dis, emptyPage, td);

            dis.close();
        } catch (IOException e) {
            throw new PageException(e);
        }
    }

    private static ArrayList<Integer> readHeader(DataInputStream dis, SlottedPage emptyPage) {
        int numSlots = emptyPage.getNumSlots();
        int headerSize = getHeaderSize(numSlots); // number of bytes in headerByteArr

        ArrayList<Integer> headerSlotsInUse = new ArrayList<>();
        for (int i = 0; i < headerSize*8; i++) {
            headerSlotsInUse.add(0);
        }

        // Read bytes into array
        byte[] headerByteArr = new byte[headerSize];
        try {
            for (int i = 0; i < headerSize; i++) {
                headerByteArr[i] = dis.readByte();
            }
        } catch (Exception e) {
            throw new PageException(e);
        }

        // read byte array to build record of header slots in use
        for (int i = 0; i < headerSize*8; i++) {
            if (isSlotUsed(i,headerByteArr)) {
                headerSlotsInUse.set(getHeaderIndex(i), 1);
            }
        }
        return headerSlotsInUse;
    }

    private static void readPayload(ArrayList<Integer> header, DataInputStream dis, SlottedPage emptyPage, TupleDesc td) {
        int numSlots = emptyPage.getNumSlots();
        try {
            for (int i = 0; i < numSlots; i++) {
                // if slot is used, parse tuple
                if (header.get(i)==1) {
                    Tuple t = new Tuple(td);
                    for (int j = 0; j < td.numFields(); j++) {
                        Field f = td.getFieldType(j).parse(dis);
                        t.setField(j,f);
                    }
                    emptyPage.insertTuple(i,t);
                } else {
                    dis.skipBytes(td.getSize());
                }
            }
        } catch (Exception e) {
            throw new PageException(e);
        }
    }

    /**
     * Checks whether a slot in the header is used or not.  Optional helper method.
     * @param i slot index to check
     * @param header a byte header, formatted as described in the javadoc at the top.
     * @return
     */
    private static boolean isSlotUsed(int i, byte[] header) {
        int targetByte = i/8;
        int adj_i = i%8;
        int shiftAmt = 7 - adj_i;
        return (header[targetByte] >> shiftAmt & 1)==1;
    }


    /**
     * Marks a slot in the header as used or not.  Optional helper method.
     * @param i slot index
     * @param header a byte header, formatted as described in the javadoc at the top.
     * @param isUsed if true, slot should be set to 1; if false, set to 0
     */
    private static void markSlot(int i, byte[] header, boolean isUsed) {
        int idx = getHeaderIndex(i);
        if (isUsed) {
            header[idx] = 1;
        } else {
            header[idx] = 0;
        }
    }

    /**
     * Calculate the true header index for given index i
     * @param i slot index to find true index in header
     * @return true index in header byte[]
     */
    private static int getHeaderIndex(int i) {
        int blockStartidx = (i/8) * 8;
        int blockEndIdx = blockStartidx + 7;
        int idx = blockStartidx + (blockEndIdx - i);
        return idx;
    }
}
