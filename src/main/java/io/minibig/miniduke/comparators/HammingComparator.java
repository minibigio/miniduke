package io.minibig.miniduke.comparators;

import no.priv.garshol.duke.Comparator;

public class HammingComparator implements Comparator {

    @Override
    public boolean isTokenized() {
        return false;
    }

    public double compare(String s1, String s2) {
        if (s1.length() != s2.length()) {
            return -1;
        }

        int counter = 0;

        for (int i = 0; i < s1.length(); i++) {
            if (s1.charAt(i) != s2.charAt(i))
                counter++;
        }

        if (counter > 2)
            return 0;
        else if (counter > 0)
            return 0.5;
        else
            return 1;
    }
}
