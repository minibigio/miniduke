package io.minibig.miniduke.comparators;

import org.elasticsearch.test.ESTestCase;

public class ZipComparatorTests extends ESTestCase {

    public void testZipEqual() {
        ZipComparator zipComparator = new ZipComparator();
        String z1 = "17000";
        String z2 = "17000";

        assertTrue(zipComparator.compare(z1, z2) > 0.91);
    }

    public void testZipDifferent() {
        ZipComparator zipComparator = new ZipComparator();
        String z1 = "17000";
        String z2 = "86000";

        assertFalse(zipComparator.compare(z1, z2) > 0.91);
    }
}
