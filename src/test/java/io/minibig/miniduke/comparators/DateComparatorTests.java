package io.minibig.miniduke.comparators;

import org.elasticsearch.test.ESTestCase;

import java.util.logging.Logger;

public class DateComparatorTests extends ESTestCase {

    public void testCompatibleDateEquals() {
        DateComparator dateComparator = new DateComparator();
        String date1 = "2018-07-14";
        String date2 = "2018-07-14";

        assertEquals(1.0, dateComparator.compare(date1, date2), 0.0001);
    }

    public void testCompatibleDateDifferent() {
        DateComparator dateComparator = new DateComparator();
        String date1 = "2018-07-14";
        String date2 = "2017-12-14";
        Logger.getLogger("comp").info(""+dateComparator.compare(date1, date2));
        assertNotEquals(1.0, dateComparator.compare(date1, date2), 0.0001);
    }
}
