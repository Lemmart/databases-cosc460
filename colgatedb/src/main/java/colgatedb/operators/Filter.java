package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
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
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private Predicate p;
    private DbIterator child;
    private DbIterator[] children;
    private boolean open;
    private Tuple nextTuple;


    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
        this.children = new DbIterator[1];
        this.children[0] = child;
        open = false;
    }

    public Predicate getPredicate() {
        return this.p;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.child.open();
        this.open = true;
    }

    @Override
    public void close() {
        this.open = false;
        this.child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (nextTuple != null) {
            return true;
        }
        while (this.open && this.child.hasNext()) {
            Tuple t = this.child.next();
            if (this.p.filter(t)) {
                nextTuple = t;
                return true;
            }
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("no more tuples!");
        }
        Tuple temp = nextTuple;
        nextTuple = null;
        return temp;


    }


    @Override
    public DbIterator[] getChildren() {
        return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new DbException("Expected only one child!");
        }
        this.children = children;
        this.child = children[0];
    }

}
