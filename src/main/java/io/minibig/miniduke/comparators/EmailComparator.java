package io.minibig.miniduke.comparators;

import no.priv.garshol.duke.Comparator;
import no.priv.garshol.duke.comparators.Levenshtein;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EmailComparator implements Comparator {
    private Levenshtein levenshtein = new Levenshtein();

    public boolean isTokenized() {
        return true; // I guess?
    }

    public double compare(String mail, String mail2) {
        if (mail.contains("@") && mail2.contains("@")) {
            String[] splitMail1 = mail.split("@");
            String name1 = splitMail1[0];

            String domain1 = null;
            String ext1 = null;
            if (splitMail1[1].contains(".")) {
                domain1 = (splitMail1[1].split("\\."))[0];
                ext1 = (splitMail1[1].split("\\."))[1];
            }

            String[] splitMail2 = mail2.split("@");
            String name2 = splitMail2[0];

            String domain2 = null;
            String ext2 = null;
            if (splitMail2[1].contains(".")) {
                domain2 = (splitMail2[1].split("\\."))[0];
                ext2 = (splitMail2[1].split("\\."))[1];
            }

            double scoreName = this.levenshtein.compare(name1, name2);

            double scoreDomain = 0.0;
            if (domain1 != null && domain2 != null)
                scoreDomain = this.levenshtein.compare(domain1, domain2);

            if (ext1 != null && ext1.equals(ext2)) {
                if (scoreDomain > 0.5) {
                    return scoreName;
                }
            }
        }

        return 0;
    }
}
