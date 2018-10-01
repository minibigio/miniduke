package io.minibig.miniduke.core;

import no.priv.garshol.duke.Record;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;

/**
 * We made this class to format as we wanted, the different values
 */
public class MinidukeRecord implements Record {

    private ArrayList<String> properties = new ArrayList<>();
    private ArrayList<ArrayList<Object>> values = new ArrayList<>();

    private ArrayList<String> suspiciousProperties = new ArrayList<>();
    private ArrayList<ArrayList<Object>> suspiciousValues = new ArrayList<>();

    public void setSuspiciousRecord(boolean suspiciousRecord) {
        this.suspiciousRecord = suspiciousRecord;
    }

    private boolean suspiciousRecord = false;

    private static Logger staticLogger = ESLoggerFactory.getLogger("MiniDuke Record");

    public MinidukeRecord() {

    }

    public String getESId() {
        if (properties.contains("id")) {
            int index = properties.indexOf("id");
            if (index > -1) {
                ArrayList<Object> ids = values.get(index);
                for (Object id : ids) {
                    if (!"ingest".equals(id))
                        return (String)id;
                }
            }
        }

        return "";
    }

    /*
     * Set the values comming from ES
     *
     * @param json Map<String, Object> : json String transformed to map
     */
    public void setValues(Map<String, ArrayList<Object>> json) {

        for (Entry<String, ArrayList<Object>> thisEntry : json.entrySet()) {
            String key = thisEntry.getKey();
            ArrayList<Object> values = thisEntry.getValue();
            ArrayList<Object> newVals = new ArrayList<>();

            for (Object value : values) {

                if (value != null) {
                    String val = String.valueOf(value);
                    if (!val.equals(""))
                        newVals.add(val);
                }
            }

            this.properties.add(key);
            this.values.add(newVals);
        }
    }

    @Override
    public Collection<String> getProperties() {
        if (!suspiciousRecord)
            return this.properties;
        else
            return this.suspiciousProperties;
    }

    public ArrayList<ArrayList<Object>> getAllValues() {
        if (!suspiciousRecord)
            return this.values;
        else
            return this.suspiciousValues;
    }

    @Override
    public Collection<String> getValues(String s) {
        List<String> props = (this.suspiciousRecord) ? this.suspiciousProperties:this.properties;
        int i = props.indexOf(s);

        ArrayList<String> list = new ArrayList<>();

        if (i > -1) {
            List<Object> values = (this.suspiciousRecord) ? this.suspiciousValues.get(i):this.values.get(i);
            for (Object sub : values) {
                list.add(sub.toString());
            }
        }

        return list;
    }

    public Map<String, ArrayList<Object>> getMappedValues() {
        Map<String, ArrayList<Object>> newMap = new HashMap<>();

        for (int i=0; i<this.properties.size(); i++) {
            newMap.put(this.properties.get(i), this.values.get(i));
        }

        return newMap;
    }

    @Override
    public String getValue(String s) {
        return null;
    }

    @Override
    public void merge(Record record) {

    }

    public void mergeMiniduke(MinidukeRecord record) {
        Map<String, ArrayList<Object>> mapRec = record.getMappedValues();

        for (Entry<String, ArrayList<Object>> thisEntry : mapRec.entrySet()) {
            String key = thisEntry.getKey();
            ArrayList<Object> value = thisEntry.getValue();

            if (!this.properties.contains(key)) {
                this.properties.add(key);
                this.values.add(value);
            } else {
                int pos = this.properties.indexOf(key);
                if (pos > -1) {

                    for (Object val : value) {

                        // Val can't be null but can be " " (cf cast)
                        if (!val.equals(" ") && !this.values.get(pos).contains(val)) {
                            this.values.get(pos).add(val);
                        }
                    }
                }
            }
        }
    }

    /*public Map<String, Object> makeSuspiciousSource(MinidukeRecord record) {
        Map<String, Object> suspiciousMap = new HashMap<>();
        suspiciousMap.put("1", this.makeSource());
        suspiciousMap.put("2", record.makeSource());
        return suspiciousMap;
    }*/

    public void makeSuspiciousSource(MinidukeRecord record) {

        this.suspiciousValues.addAll(this.getAllValues());
        this.suspiciousValues.addAll(record.getAllValues());

        List<String> props = new ArrayList<>();
        for (String prop : this.properties) {
            props.add("1."+prop);
        }
        this.suspiciousProperties.addAll(props);

        props.clear();
        for (String prop : record.properties) {
            props.add("2."+prop);
        }
        this.suspiciousProperties.addAll(props);

        // Set as suspicious at the end to still have access to the values using the getter
        this.suspiciousRecord = true;
    }

    public static Map<String, ArrayList<Object>> formatValuesToMap(Map<String, Object> map) {
        Map<String, ArrayList<Object>> newMap = new HashMap<>();

        for (Entry<String, Object> thisEntry : map.entrySet()) {

            if (!(thisEntry.getValue() instanceof ArrayList<?>)) {
                ArrayList<Object> value = new ArrayList<>();
                value.add(thisEntry.getValue());
                newMap.put(thisEntry.getKey(), value);
            }
            else {
                if (thisEntry.getValue() instanceof ArrayList) {
                    ArrayList<Object> values = new ArrayList<>((ArrayList<?>)thisEntry.getValue());

                    newMap.put(thisEntry.getKey(), values);
                }
            }

        }

        return newMap;
    }

    public Map<String, Object> makeSource() {
        Map<String, Object> sourceMap = new HashMap<>();

        for (int i=0; i<this.properties.size(); i++) {
            if (!this.properties.get(i).equals("id"))
                sourceMap.put(this.properties.get(i), this.values.get(i));
        }

        return sourceMap;
    }

}
