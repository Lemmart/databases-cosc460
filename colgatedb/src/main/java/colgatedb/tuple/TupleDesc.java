package colgatedb.tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A helper class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;

    private ArrayList<TDItem> TDArrayList;

    /**
     * Create a new TupleDesc with typeAr.length fields of the
     * specified types and with associated names.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        TDArrayList = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            TDArrayList.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types.  Field names should be assigned as empty
     * strings.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        TDArrayList = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], "");
            TDArrayList.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return TDArrayList.size();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < TDArrayList.size() && i >= -1) {
            return TDArrayList.get(i).fieldType;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < TDArrayList.size() && i >= 0) {
            return TDArrayList.get(i).fieldName;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < TDArrayList.size(); i++) {
            String fieldName = TDArrayList.get(i).fieldName;
            if (fieldName != null && fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.  The size
     * depends on the field types.
     *
     * @see Type#getLen()
     */
    public int getSize() {
        int size = 0;
        for (TDItem item: TDArrayList) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return TDArrayList.iterator();
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        TupleDesc TDObject;
        if (o instanceof TupleDesc) {
            TDObject = ((TupleDesc)o);
        } else {
            return false;
        }

        if (this.getSize() == TDObject.getSize()) {
            for (int i = 0; i < TDArrayList.size(); i++) {
                if (TDArrayList.get(i).fieldType != TDObject.TDArrayList.get(i).fieldType) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results.  If you're not
        // sure yet (you may not be), then leave the UnsupportedOperationException
        // so that if this method ever gets called, it will trigger an exception.
        return TDArrayList.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])".
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String string = "";
        int count = 1;
        int length = TDArrayList.size();
        for (TDItem item : TDArrayList) {
            string += item.fieldName + "(" + item.fieldType + ")";
            if (count < length) {
                string += ", ";
            }
            count++;
        }
        return string;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] types = new Type[1];
        types[0] = Type.INT_TYPE;                      // build placeholder element
        TupleDesc TDNew = new TupleDesc(types);
        TDNew.TDArrayList.addAll(td1.TDArrayList);
        TDNew.TDArrayList.addAll(td2.TDArrayList);
        TDNew.TDArrayList.remove(0);            // remove placeholder element
        return TDNew;
    }


}
