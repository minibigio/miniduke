package io.minibig.miniduke.ingest;

import io.minibig.miniduke.core.MinidukeDatasource;
import no.priv.garshol.duke.DataSource;
import no.priv.garshol.duke.Property;
import no.priv.garshol.duke.Record;

import org.elasticsearch.SpecialPermission;

import io.minibig.miniduke.core.MinidukeRecord;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The main MiniDuke class for all sorts of actions which cannot be elsewhere
 */
public class Miniduke {

    /**
     * Sets a map to a (Miniduke)Record
     *
     * @param ingest the ingested data
     * @return       a record
     */
    public static MinidukeRecord recordFromIngest(Map<String, Object> ingest) {
        Map<String, ArrayList<Object>> newMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : ingest.entrySet()) {
            ArrayList<Object> array = new ArrayList<>();

            if (entry.getValue() instanceof String) {
                String en = String.valueOf(entry.getValue());
                Pattern pattern = Pattern.compile("\\[[\\w,]+, *\\w+\\]");
                Matcher matcher = pattern.matcher(en);
                if (matcher.find()) {
                    en = en.replace("[", "");
                    en = en.replace("]", "");
                    String[] explode = en.split(",");
                    for (String exploded : explode) {
                        array.add(exploded.trim());
                    }
                }
                else
                    array.add(entry.getValue());
            }


            newMap.put(entry.getKey(), array);
        }

        MinidukeRecord record = new MinidukeRecord();
        record.setValues(newMap);

        return record;
    }

    /**
     * Adds only one Record into a (Miniduke)DataSource
     * Method made for the ingested values
     *
     * @param record a record made for the ingested data
     * @return       the datasource
     */
    public static DataSource sourceFromIngest(Record record) {
        Collection<MinidukeRecord> records = new ArrayList<>();
        records.add((MinidukeRecord)record);
        return new MinidukeDatasource(records);
    }

    /**
     * Deletes the properties associated to empty or null values
     * These values mustn't be compared
     *
     * @param values     the values
     * @param properties the original list of properties
     * @return           the new list of properties
     * @deprecated       this part is already done natively in Duke
     */
    @Deprecated
    public static List<Property> checkValuesProperties(Map<String, Object> values, List<Property> properties) {

        boolean remove = false;
        for (Map.Entry<String, Object> valueList : values.entrySet()) {

            if (valueList.getValue() instanceof String) {
                String val = (String) valueList.getValue();
                if (val.equals(""))
                    remove = true;
            }

            if (valueList.getValue() instanceof List) {
                List<?> valA = (List<?>) valueList.getValue();
                if (valA.size() > 0) {
                    for (Object val : valA) {
                        if (val == null || (val instanceof String && val.equals("")))
                            remove = true;
                    }
                }
                else
                    remove = true;
            }

            int i = 0;
            for (Property prop : properties) {
                if (prop.getName().equals(valueList.getKey()))
                    properties.get(i).setIgnoreProperty(remove);
                i++;
            }

            remove = false;
        }

        return properties;
    }

    /**
     * Checks if both records contain the same values
     *
     * @param rec    a record
     * @param ingest the ingested record
     * @deprecated   will be soon removed for being useless
     * @return       true if one contains the other
     */
    @Deprecated
    public static boolean contains(MinidukeRecord rec, MinidukeRecord ingest) {
        boolean contains = false;
        Map<String, ArrayList<Object>> delIdRec = rec.getMappedValues();
        delIdRec.keySet().removeIf(key -> (key.equals("id")));

        Map<String, ArrayList<Object>> delIdIngest = ingest.getMappedValues();
        delIdIngest.keySet().removeIf(key -> (key.equals("id")));

        if (rec.getProperties().size() <= ingest.getProperties().size()) {
            contains = delIdIngest.entrySet().containsAll(delIdRec.entrySet());
        }
        else if (rec.getProperties().size() >= ingest.getProperties().size()) {
            contains = delIdRec.entrySet().containsAll(delIdIngest.entrySet());
        }

        return contains;
    }

    /**
     * Casts non optional ingest values (see castStringIngestValues)
     *
     * @param fieldName          the field name in case of an error
     * @param ingest             the ingest raw values
     * @return                   the list of casted values
     * @throws MinidukeException if a field cannot be casted
     */
    public ArrayList<String> castStringIngestValues(String fieldName, List<?> ingest) throws MinidukeException {
        return castStringIngestValues(fieldName, ingest, false);
    }

    /**
     * Casts ingest values which are not defined yet as List of String
     * Every data must be casted to strings to be compared
     * This is a general case
     *
     * @param fieldName          the field name in case of an error
     * @param ingest             the ingest raw values
     * @param optional           the value of a field can be set to null
     * @return                   the list of casted values
     * @throws MinidukeException if a field cannot be casted
     */
    public ArrayList<String> castStringIngestValues(String fieldName, List<?> ingest, boolean optional) throws MinidukeException {
        ArrayList<String> ingestA = new ArrayList<>();

        for (Object obj : ingest) {
            if (!optional) {
                if (obj instanceof String) {
                    ingestA.add((String) obj);
                } else
                    throw new MinidukeException("Unable to cast [" + fieldName + "]");
            }
            else {
                if (obj != null) {
                    String val = String.valueOf(obj).trim();

                    if (!val.equals(""))
                        ingestA.add(val);
                    else
                        ingestA.add(null);
                } else
                    ingestA.add(null);
            }
        }

        return ingestA;
    }

    /**
     * Casts the weight field ingested to a double array meaning the low and high values
     *
     * @param ingest             the ingest raw values
     * @return                   the list of casted values
     * @throws MinidukeException if a field cannot be casted
     */
    public ArrayList<double[]> castWeightIngestValues(List<?> ingest) throws MinidukeException {
        ArrayList<double[]> weightA = new ArrayList<>();

        int weightI = 0;
        for (Object weight : ingest) {

            if (weight instanceof String) {
                if (((String) weight).matches("\\[\\d\\.\\d+,[ ]*\\d\\.\\d+]")) {
                    String weightNew = ((String) weight).substring(1, ((String) weight).length() - 1);
                    String[] weightNewT = weightNew.split(",");
                    if (weightNewT.length == 2) {
                        List<Double> weightC = new ArrayList<>();
                        weightC.add(Double.parseDouble(weightNewT[0]));
                        weightC.add(Double.parseDouble(weightNewT[1]));
                        weight = weightC;
                    }
                }
            }

            if (weight instanceof List<?>) {
                List<?> weightList = (ArrayList) weight;
                double[] d = new double[2];

                for (Object weightSub : weightList) {
                    d[weightI] = (double) weightSub;
                    weightI++;
                }

                weightA.add(d);
            } else
                throw new MinidukeException("Unable de cast [Weight]");

            weightI = 0;
        }

        return weightA;
    }

    /**
     * This class lets us have access to an outdoor connection (Rest Client)
     * Thanks to David Pilato, Elasticsearch team
     */
    public static class ClientSecurityManager {
        public static final SpecialPermission INSTANCE = new SpecialPermission();

        public static <T> T doPrivilegedException(PrivilegedExceptionAction<T> operation) throws Exception {
            check();
            try {
                return AccessController.doPrivileged(operation);
            } catch (PrivilegedActionException e) {
                throw (Exception) e.getCause();
            }
        }

        // Stolen from the SpecialPermission class (ES v6.x)
        public static void check() {
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkPermission(INSTANCE);
            }
        }

    }

}
