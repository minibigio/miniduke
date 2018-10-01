package io.minibig.miniduke.comparators;

import no.priv.garshol.duke.Comparator;
import no.priv.garshol.duke.comparators.ExactComparator;
import no.priv.garshol.duke.comparators.Levenshtein;
import no.priv.garshol.duke.comparators.NumericComparator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PhoneComparator implements Comparator {

    private Levenshtein levenshtein = new Levenshtein();
    private NumericComparator numeric = new NumericComparator();
    private ExactComparator exact = new ExactComparator();

    @Override
    public boolean isTokenized() {
        return false;
    }

    public double compare(String phone1, String phone2) {
        // Replace French telephone code
        phone1 = phone1.replace("+33", "0");
        phone2 = phone2.replace("+33", "0");

        // Clean the phone number
        Pattern p = Pattern.compile("[^0-9]");
        phone1 = p.matcher(phone1).replaceAll("");
        phone2 = p.matcher(phone2).replaceAll("");

        // Other telephone codes
        Pattern pattern = Pattern.compile("(\\+[0-9]{2})");
        Matcher matcher1 = pattern.matcher(phone1);
        Matcher matcher2 = pattern.matcher(phone2);

        if (matcher1.find() && matcher2.find()) {
            return 0.5; // How to deal with other telephone numbers ?
        } else if ((!matcher1.find() && matcher2.find()) || (matcher1.find() && !matcher2.find())) {
            return 0;
        }

        // Both french telephones
        return new HammingComparator().compare(phone1, phone2);
    }

}
