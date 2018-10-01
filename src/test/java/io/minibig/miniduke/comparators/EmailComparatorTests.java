package io.minibig.miniduke.comparators;

import org.elasticsearch.test.ESTestCase;

public class EmailComparatorTests extends ESTestCase {

    public void testEmailEqual() {
        EmailComparator emailComparator = new EmailComparator();
        String e1 = "no-reply@falsedomain.mma";
        String e2 = "no-reeply@falsedommain.mma";

        assertTrue(emailComparator.compare(e1, e2) > 0.91);
    }

    public void testEmailDifferent() {
        EmailComparator emailComparator = new EmailComparator();
        String e1 = "no-reply@falsedomain.mna";
        String e2 = "noreply@falsedommain.mma";

        // A different extension returns 0
        assertFalse(emailComparator.compare(e1, e2) > 0.91);
    }
}
