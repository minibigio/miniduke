package io.minibig.miniduke.core;

import no.priv.garshol.duke.DataSource;
import no.priv.garshol.duke.Logger;
import no.priv.garshol.duke.Record;
import no.priv.garshol.duke.RecordIterator;
import no.priv.garshol.duke.utils.DefaultRecordIterator;

import java.util.ArrayList;
import java.util.Collection;

public class MinidukeDatasource implements DataSource {
    private Collection<Record> records = new ArrayList<>();

    public MinidukeDatasource(Collection<MinidukeRecord> records) {
        this.records.addAll(records);
    }


    @Override
    public RecordIterator getRecords() {
        return new DefaultRecordIterator(this.records.iterator());
    }

    @Override
    public void setLogger(Logger logger) {

    }

}
