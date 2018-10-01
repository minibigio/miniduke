package io.minibig.miniduke.comparators;

import no.priv.garshol.duke.Comparator;
import no.priv.garshol.duke.comparators.NumericComparator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

public class DateComparator implements Comparator {

    private static final String FORMAT_HYPHEN_YMD = "YYYY-MM-DD";
    private static final String FORMAT_HYPHEN_DMY = "DD-MM-YYYY";

    private static final String FORMAT_SLASH_YMD = "YYYY/MM/DD";
    private static final String FORMAT_SLASH_DMY = "DD/MM/YYYY";

    private SimpleDateFormat dateFormat = new SimpleDateFormat("", Locale.FRANCE);

    @Override
    public boolean isTokenized() {
        return false;
    }

    public Date parseDate(String date) throws Exception {
        try {
            dateFormat.applyPattern(FORMAT_HYPHEN_YMD);
            return dateFormat.parse(date);
        } catch (ParseException e) { }

        try {
            dateFormat.applyPattern(FORMAT_HYPHEN_DMY);
            return dateFormat.parse(date);
        } catch (ParseException e) { }

        try {
            dateFormat.applyPattern(FORMAT_SLASH_YMD);
            return dateFormat.parse(date);
        } catch (ParseException e) { }

        try {
            dateFormat.applyPattern(FORMAT_SLASH_DMY);
            return dateFormat.parse(date);
        } catch (ParseException e) { }

        throw new Exception("Wrong date format");
    }

    @Override
    public double compare(String v1, String v2) {

        try {
            Date date1 = this.parseDate(v1);
            Date date2 = this.parseDate(v2);

            Calendar cal1 = new GregorianCalendar(TimeZone.getTimeZone("Europe/Paris"), Locale.FRANCE);
            cal1.setTime(date1);

            Calendar cal2 = new GregorianCalendar(TimeZone.getTimeZone("Europe/Paris"), Locale.FRANCE);
            cal2.setTime(date2);

            NumericComparator num = new NumericComparator();

            double yearComp = num.compare(""+cal1.get(Calendar.YEAR), ""+cal2.get(Calendar.YEAR));
            double monthComp = num.compare(""+cal1.get(Calendar.MONTH), ""+cal2.get(Calendar.MONTH));
            double dayComp = num.compare(""+cal1.get(Calendar.DAY_OF_MONTH), ""+cal2.get(Calendar.DAY_OF_MONTH));

            return (yearComp + monthComp + dayComp) / 3;

        } catch (Exception e) { }

        return 0.00;
    }
}
