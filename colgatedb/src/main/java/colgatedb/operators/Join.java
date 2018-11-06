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
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private Tuple tuple1;
    private Tuple tuple2;
    private boolean open;
    private TupleDesc td;
    private Tuple mergedTup;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.td = TupleDesc.merge(child1.getTupleDesc(),child2.getTupleDesc());
        setTupleDesc(td);
    }

    public JoinPredicate getJoinPredicate() {
        return p;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
        open = true;
    }

    @Override
    public void close() {
        child1.close();
        child2.close();
        open = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if (!open) {
            throw new DbException("[ERROR] Unable to rewind: Not open!");
        }
        child1.rewind();
        child2.rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (mergedTup != null && open) {
            return true;
        }

        while (child1.hasNext() || tuple1 != null) {
            if (tuple1 == null) {
                tuple1 = child1.next();
                child2.rewind();
            }
            while (child2.hasNext()) {
                tuple2 = child2.next();
                if (p.filter(tuple1,tuple2)) {
                    mergedTup = new Tuple(td);
                    int tupIdx = 0;
                    for (int i = 0; i < tuple1.getTupleDesc().numFields(); i++) {
                        mergedTup.setField(tupIdx, tuple1.getField(i));
                        tupIdx++;
                    }
                    for (int i = 0; i < tuple2.getTupleDesc().numFields(); i++) {
                        mergedTup.setField(tupIdx, tuple2.getField(i));
                        tupIdx++;
                    }
                    return true;
                }
            }
            child2.rewind();
            tuple1 = null;
        }
        return false;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. The implementation is a simple nested loops join.
     * <p/>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p/>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("No more tuples!");
        }
        Tuple t = mergedTup;
        mergedTup = null;
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child1,this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) throws DbException {
        if (children.length != 2) {
            throw new DbException("[ERROR] Join.java: Failed to set children with array of length " + children.length);
        }
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
