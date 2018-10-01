package io.minibig.miniduke.core;

import no.priv.garshol.duke.Record;
import no.priv.garshol.duke.matchers.MatchListener;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class MinidukeMatchListener implements MatchListener {
    private Map<Record, Record> recMatches = new LinkedHashMap<>();
    private Map<Record, Record> maybeMatches = new LinkedHashMap<>();
    private ArrayList<Double> distance = new ArrayList<>();
    private ArrayList<Double> maybeDistance = new ArrayList<>();
    private ArrayList<Record> noMatches = new ArrayList<>();



    private static Logger staticLogger = ESLoggerFactory.getLogger("MiniDuke Match Listener");

    public Map<Record, Record> getMatches() {
        return this.recMatches;
    }

    public Map<Record, Record> getMaybeMatches() {
        return maybeMatches;
    }

    public ArrayList<Double> getMaybeDistance() {
        return maybeDistance;
    }

    public ArrayList<Double> getDistance() {
        return this.distance;
    }

    public ArrayList<Record> getNoMatches() { return this.noMatches; }


    @Override
    public void batchReady(int i) {

    }

    @Override
    public void batchDone() {

    }

    @Override
    public void matches(Record record1, Record record2, double v) {
        this.recMatches.put(record1, record2);
        if (this.recMatches.size() > this.distance.size())
            this.distance.add(v);
    }

    @Override
    public void matchesPerhaps(Record record1, Record record2, double v) {
        this.maybeMatches.put(record1, record2);
        if (this.maybeMatches.size() > this.maybeDistance.size())
            this.maybeDistance.add(v);
    }

    @Override
    public void noMatchFor(Record record) {
        this.noMatches.add(record);
    }

    @Override
    public void startProcessing() {

    }

    @Override
    public void endProcessing() {

    }


}
