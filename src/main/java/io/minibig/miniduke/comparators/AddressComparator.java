package io.minibig.miniduke.comparators;

import no.priv.garshol.duke.Comparator;
import no.priv.garshol.duke.comparators.ExactComparator;
import no.priv.garshol.duke.comparators.Levenshtein;
import no.priv.garshol.duke.comparators.NumericComparator;

import java.util.ArrayList;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AddressComparator implements Comparator {

    private Comparator levenshtein = new Levenshtein();
    private Comparator numeric = new NumericComparator();
    private ExactComparator exact = new ExactComparator();

    @Override
    public boolean isTokenized() {
        return false;
    }

    public double compare(String adr1, String adr2) {
        adr1 = adr1.replace(",", "").trim();
        adr2 = adr2.replace(",", "").trim();

        ArrayList<Double> marks = new ArrayList<>();
        int nbMarks = 0;

        Pattern pattern = Pattern.compile("([0-9]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher1 = pattern.matcher(adr1);
        Matcher matcher2 = pattern.matcher(adr2);

        if (matcher1.find() && matcher2.find()) { // If "numero de rue"
            // Using Levenshtein and not numeric because we want it to be considered as text
            // If you forget one character (e.g. 497 instead of 4970), the distance will only be 1
            double nbRoad = levenshtein.compare(matcher1.group(1), matcher2.group(1));
            marks.add(nbRoad);
            nbMarks++;
        }


        pattern = Pattern.compile("[0-9]+(bis|ter){1}", Pattern.CASE_INSENSITIVE);
        matcher1 = pattern.matcher(adr1);
        matcher2 = pattern.matcher(adr2);
        if (matcher1.find() && matcher2.find()) { // If bis, ter
            double nbRoad = exact.compare(matcher1.group(1), matcher2.group(1));

            marks.add(nbRoad);
            nbMarks++;
        }


        pattern = Pattern.compile("[0-9]+\\s*(\\w+)", Pattern.CASE_INSENSITIVE);
        matcher1 = pattern.matcher(adr1);
        matcher2 = pattern.matcher(adr2);
        // @TODO: Match Boulevard and Bd or Bvd for example
        if (matcher1.find() && matcher2.find()) { // If type road
            double typeRoad = levenshtein.compare(matcher1.group(1).toUpperCase(Locale.CANADA_FRENCH),
                    matcher2.group(1).toUpperCase(Locale.CANADA_FRENCH));
            marks.add(typeRoad);
            nbMarks++;
        }


        pattern = Pattern.compile("[0-9]+\\s*\\w+\\s*(\\w+)", Pattern.CASE_INSENSITIVE);
        matcher1 = pattern.matcher(adr1);
        matcher2 = pattern.matcher(adr2);

        if (matcher1.find() && matcher2.find()) { // If name of the road
            double nameRoad = levenshtein.compare(matcher1.group(1).toUpperCase(Locale.CANADA_FRENCH),
                    matcher2.group(1).toUpperCase(Locale.CANADA_FRENCH));
            marks.add(nameRoad);
            nbMarks++;
        }


        double score = 0;

        for (Double d : marks)
            score += d;

        return (score > 0) ? score / nbMarks : 0;
    }
}
