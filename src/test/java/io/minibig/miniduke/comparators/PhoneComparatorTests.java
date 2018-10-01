package io.minibig.miniduke.comparators;

import org.elasticsearch.test.ESTestCase;

public class PhoneComparatorTests extends ESTestCase {

    public void testPhoneEqual() {
        PhoneComparator phoneComparator = new PhoneComparator();
        String p1 = "01 23 45 67 89";
        String p2 = "+33123456789";

        assertTrue(phoneComparator.compare(p1, p2) > 0.91);
    }

    public void testPhoneDifferent() {
        PhoneComparator phoneComparator = new PhoneComparator();
        String p1 = "01 24 45 67 89";
        String p2 = "+33123456789";

        assertFalse(phoneComparator.compare(p1, p2) > 0.91);
    }
}
