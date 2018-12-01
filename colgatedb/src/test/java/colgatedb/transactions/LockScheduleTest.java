package colgatedb.transactions;

import colgatedb.page.SimplePageId;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import javax.sound.midi.SysexMessage;

import static org.junit.Assert.*;

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
public class LockScheduleTest {
    private TransactionId tid0 = new TransactionId();
    private TransactionId tid1 = new TransactionId();
    private TransactionId tid2 = new TransactionId();
    private TransactionId tid3 = new TransactionId();
    private SimplePageId pid1 = new SimplePageId(0, 1);
    private SimplePageId pid2 = new SimplePageId(0, 2);
    private SimplePageId pid3 = new SimplePageId(0, 3);
    private SimplePageId pid4 = new SimplePageId(0, 4);
    private LockManager lm;
    private Schedule.Step[] steps;
    private Schedule schedule;

    @Before
    public void setUp() {
        lm = new LockManagerImpl();
    }

    @Test
    @GradedTest(number="19.1", max_score=1.0, visibility="visible")
    public void acquireLock() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),
                // important detail: acquired step must be included in schedule and should appear as soon as the
                // lock is acquired.  in this case, the lock is acquired immediately.
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED)
        };
        executeSchedule();
    }

    /**
     * Tricky test case:
     * - T1 has shared lock and T2 waiting on exclusive
     * - then T1 requests upgrade, it should be granted because upgrades get highest priority
     */
    @Test
    @GradedTest(number="19.2", max_score=1.0, visibility="visible")
    public void upgradeRequestCutsInLine() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.EXCLUSIVE),  // t2 waiting for exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),  // t1 requests upgrade, should be able to cut line
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // t1 gets exclusive ahead of t2
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED)    // now t2 can get exclusive
        };
        executeSchedule();
    }

    // write three unit tests here

    /**
     * Unit test 1
     * T1 is holding the lock and requests it again despite T2 waiting
     * Simulates repeated modification requests on same page
     */
    @Test
    @GradedTest(number="19.3", max_score=1.0, visibility="visible")
    public void duplicateRequestBySameTid() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),   // t1 requests exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.EXCLUSIVE),   // t2 waiting for exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),   // t1 requests exclusive, should already have it
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED)     // now t2 can get exclusive
        };
        executeSchedule();
    }

    /**
     * Unit test 2
     * interleave requests for shared lock to ensure that all threads can simultaneously hold the lock
     */
    @Test
    @GradedTest(number="19.4", max_score=1.0, visibility="visible")
    public void allowMultipleShared() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),      // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),      // t2 requests shared
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid2, pid1, Schedule.Action.SHARED),      // t3 requests shared
                new Schedule.Step(tid2, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.UNLOCK)
        };
        executeSchedule();
    }

    /**
     * Unit test 3
     * Acquire locks on multiple pages using same transaction ID
     */
    @Test
    @GradedTest(number="19.5", max_score=1.0, visibility="visible")
    public void singleTidMultiplePages() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // acquire lock on page 1
                new Schedule.Step(tid0, pid2, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid0, pid2, Schedule.Action.ACQUIRED),   // acquire lock on page 2
                new Schedule.Step(tid0, pid3, Schedule.Action.SHARED),
                new Schedule.Step(tid0, pid3, Schedule.Action.ACQUIRED),   // acquire lock on page 3
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid0, pid2, Schedule.Action.UNLOCK),
                new Schedule.Step(tid0, pid3, Schedule.Action.UNLOCK)
        };
        executeSchedule();
    }

    /**
     * Unit test 4
     * Acquire locks on multiple pages using multiple transaction ID's
     * Ensure that t3 waits on t2 before acquiring the lock and then subsequently releasing it
     */
    @Test
    @GradedTest(number="19.6", max_score=1.0, visibility="visible")
    public void multipleTidsMultiplePages() {
        steps = new Schedule.Step[]{

                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // t1 acquire lock on page 1

                new Schedule.Step(tid1, pid2, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid1, pid2, Schedule.Action.ACQUIRED),   // t2 acquire lock on page 2

                new Schedule.Step(tid3, pid2, Schedule.Action.EXCLUSIVE),

                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid2, Schedule.Action.UNLOCK),
                new Schedule.Step(tid3, pid2, Schedule.Action.ACQUIRED),   // t3 requests lock on page 2
                new Schedule.Step(tid3, pid2, Schedule.Action.UNLOCK)
        };
        executeSchedule();
    }

    private void executeSchedule() {
        try {
            schedule = new Schedule(steps, lm);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(schedule.allStepsCompleted());
    }
}
