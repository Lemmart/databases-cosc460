package colgatedb.operators;

import colgatedb.Catalog;
import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.dbfile.DbFile;
import colgatedb.dbfile.DbFileIterator;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

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
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private TransactionId tid;
    private int tableid;
    private final String tableAlias;
    private String tableName;
    private TupleDesc td;
    private DbFileIterator dbFileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the id of the table to scan.  The actual table can be retrieved from the Catalog.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        tableName = Database.getCatalog().getTableName(tableid);
        this.tableAlias = tableAlias;

        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        dbFileIterator = dbFile.iterator(tid);

//        Build new tupledesc
        TupleDesc tdTemp = dbFile.getTupleDesc();
        int numFields = tdTemp.numFields();
        Type [] types = new Type[numFields];
        String [] fieldNames = new String[numFields];
//        if (tableAlias == "") {
//            tableAlias = "null";
//        }
        for (int i = 0; i < numFields; i++) {
//            if (tdTemp.getFieldName(i) == "") {
//                fieldNames[i] = tableAlias + '.' + "null";
//            } else {
//                fieldNames[i] = tableAlias + '.' + tdTemp.getFieldName(i);
//            }
            fieldNames[i] = tableAlias + '.' + tdTemp.getFieldName(i);
        }
        td = new TupleDesc(types,fieldNames);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return this.tableAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return this.dbFileIterator.next();
    }

    public void close() {
        this.dbFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.dbFileIterator.close();
    }
}
