package io.minibig.miniduke.comparators;

import org.elasticsearch.test.ESTestCase;

public class AddressComparatorTests extends ESTestCase {

    public void testAddressLvlOneEqual() {
        AddressComparator addressComparator = new AddressComparator();
        String a1 = "12 Avn du 11 novembre, 17000 La Rochelle";
        String a2 = "12 Avn du 11 novenbre, 17009 La Rochelle";

        assertTrue(addressComparator.compare(a1, a2) > 0.9);
    }

    public void testAddressLvlOneDifferent() {
        AddressComparator addressComparator = new AddressComparator();
        String a1 = "12 Avn du 11 novembre, 17000 La Rochelle";
        String a2 = "2 Avn du 11 novenbre, 17009 La Rochelle";

        // The house number is important !
        assertFalse(addressComparator.compare(a1, a2) > 0.9);
    }
}
