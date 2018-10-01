package io.minibig.miniduke.comparators;

import no.priv.garshol.duke.Comparator;
import no.priv.garshol.duke.comparators.ExactComparator;
import no.priv.garshol.duke.comparators.NumericComparator;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZipComparator implements Comparator {

    private NumericComparator numeric = new NumericComparator();
    private ExactComparator exact = new ExactComparator();

    @Override
    public boolean isTokenized() {
        return false;
    }

    @Override
    public double compare(String adr1, String adr2) {
        adr1 = adr1.trim();
        adr2 = adr2.trim();

        ArrayList<Double> marks = new ArrayList<>();
        int nbMarks = 0;

        Pattern pattern = Pattern.compile("([0-9]{2})", Pattern.CASE_INSENSITIVE);
        Matcher matcher1 = pattern.matcher(adr1);
        Matcher matcher2 = pattern.matcher(adr2);

        if (matcher1.find() && matcher2.find()) { // If dep number
            double deptNb = exact.compare(matcher1.group(1), matcher2.group(1));
            marks.add(deptNb);
            nbMarks++;
        }
        else
            return 0;


        pattern = Pattern.compile("[0-9]{2}([0-9]{3})", Pattern.CASE_INSENSITIVE);
        matcher1 = pattern.matcher(adr1);
        matcher2 = pattern.matcher(adr2);

        if (matcher1.find() && matcher2.find()) { // If other number
            double otherNb = numeric.compare(matcher1.group(1), matcher2.group(1));
            marks.add(otherNb);
            nbMarks++;
        }
        else
            return 0;


        double score = 0;

        for (Double d : marks)
            score += d;

        return (score > 0) ? score/nbMarks:0;
    }
}
