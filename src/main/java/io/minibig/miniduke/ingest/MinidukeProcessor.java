package io.minibig.miniduke.ingest;

import no.priv.garshol.duke.Comparator;
import no.priv.garshol.duke.DataSource;
import no.priv.garshol.duke.Property;
import no.priv.garshol.duke.PropertyImpl;
import no.priv.garshol.duke.Record;
import org.apache.logging.log4j.Logger;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import io.minibig.miniduke.core.MinidukeConfiguration;
import io.minibig.miniduke.core.MinidukeDatasource;
import io.minibig.miniduke.core.MinidukeMatchListener;
import io.minibig.miniduke.core.MinidukeRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static io.minibig.miniduke.ingest.Miniduke.ClientSecurityManager.doPrivilegedException;

public class MinidukeProcessor extends AbstractProcessor {

    private static Logger staticLogger = ESLoggerFactory.getLogger("MiniDuke Processor");

    public static String NAME = "miniduke";

    private final String comparator;
    private final String data;
    private final String filters;
    private final String fields;
    private final String threshold;
    private final String thresholdMaybe;
    private final String weight;
    private final String host;

    protected MinidukeProcessor(String tag, String fields, String threshold, String thresholdMaybe,
                                String weight, String data, String filters, String comparator,
                                String host) {
        super(tag);

        this.comparator = comparator;
        this.data = data;
        this.filters = filters;
        this.fields = fields;
        this.threshold = threshold;
        this.thresholdMaybe = thresholdMaybe;
        this.weight = weight;
        this.host = host;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {

        staticLogger.info("Miniduke, at your service Beta #1");

        try {
            // Get the ingested data
            List<?> fieldsVal = ingestDocument.getFieldValue(this.fields, List.class);
            List<?> weightVal = ingestDocument.getFieldValue(this.weight, List.class);
            List<?> valuesVal = ingestDocument.getFieldValue(this.data, List.class);
            List<?> comparatorVal = ingestDocument.getFieldValue(this.comparator, List.class);
            Object filtersValO = ingestDocument.getFieldValue(this.filters, Object.class);
            List<String> filtersVal = new ArrayList<>();
            if (filtersValO instanceof List) {
                for (Object str : (List)filtersValO)
                    filtersVal.add(String.valueOf(str));
            }
            else {
                filtersVal.add(String.valueOf(filtersValO));
            }
            Object thresholdVal = ingestDocument.getFieldValue(this.threshold, Object.class);
            Object thresholdMaybeVal = ingestDocument.getFieldValue(this.thresholdMaybe, Object.class);
            String hostVal = ingestDocument.getFieldValue(this.host, String.class);

            // Remove them form the ingestDoc
            ingestDocument.removeField(this.fields);
            ingestDocument.removeField(this.threshold);
            ingestDocument.removeField(this.thresholdMaybe);
            ingestDocument.removeField(this.weight);
            ingestDocument.removeField(this.data);
            ingestDocument.removeField(this.comparator);
            ingestDocument.removeField(this.filters);
            ingestDocument.removeField(this.host);

            String indexVal = ingestDocument.getFieldValue("_index", String.class);

            // Cast them to map them and set a record
            ArrayList<String> fieldsA = new ArrayList<>();
            ArrayList<double[]> weightA = new ArrayList<>();
            ArrayList<String> valuesA = new ArrayList<>();
            ArrayList<String> comparatorA = new ArrayList<>();
            ArrayList<String> filtersA = new ArrayList<>();

            double thresholdA = 0.5;
            double thresholdMaybeA = 0.4;
            try {
                Miniduke minidukeTools = new Miniduke();

                fieldsA = minidukeTools.castStringIngestValues("Fields", fieldsVal);
                weightA = minidukeTools.castWeightIngestValues(weightVal);
                valuesA = minidukeTools.castStringIngestValues("Values", valuesVal, true);
                comparatorA = minidukeTools.castStringIngestValues("Comparators", comparatorVal);
                filtersA = minidukeTools.castStringIngestValues("Filters", filtersVal);

                thresholdA = (thresholdVal instanceof String) ?
                    Double.parseDouble((String) thresholdVal) : (double) thresholdVal;
                thresholdMaybeA = (thresholdMaybeVal instanceof String) ?
                    Double.parseDouble((String) thresholdMaybeVal) : (double) thresholdMaybeVal;
            }
            catch (MinidukeException e) {
                staticLogger.error(e);
            }


            Map<String, Object> ingestMap = new HashMap<>();
            ingestMap.put("id", "ingest");
            for (int i=0; i<fieldsA.size(); i++) {
                ingestMap.put(fieldsA.get(i), valuesA.get(i));
            }

            List<Property> properties = new ArrayList<>();
            Property prop_id = new PropertyImpl("id");
//                Property prop_str1 = new PropertyImpl("first_name", new Levenshtein(), 0.5, 0.7); // Property example
            properties.add(prop_id);

            for (int i= 0; i<fieldsA.size(); i++) {
                if (valuesA.get(i) != null)
                    properties.add(new PropertyImpl(fieldsA.get(i),
                            (Comparator)Class.forName(comparatorA.get(i)).newInstance(),
                            weightA.get(i)[0], weightA.get(i)[1]));
            }

            // Make the first source (Ingest) for the link
            MinidukeRecord ingestRecord = Miniduke.recordFromIngest(ingestMap);
            DataSource ingestData = Miniduke.sourceFromIngest(ingestRecord);
            Collection<DataSource> arrayIngestData = new ArrayList<>();
            arrayIngestData.add(ingestData);

            // Make config (xml equivalent)
            MinidukeConfiguration configDuke = new MinidukeConfiguration(); // duke.ConfigurationImpl
//            ConfigurationImpl configDuke = new ConfigurationImpl();
            configDuke.addDataSource(1, ingestData);
            configDuke.setThreshold(thresholdA);
            configDuke.setMaybeThreshold(thresholdMaybeA);

            properties = Miniduke.checkValuesProperties(ingestMap, properties);

            configDuke.setProperties(properties);

            // Custom match listener
            MinidukeMatchListener miniDukeML = new MinidukeMatchListener();

            // Connect to the server to query ES
            MinidukeConnection.connect(hostVal);
            RestHighLevelClient client = MinidukeConnection.client;
            staticLogger.info("Entry number "+MinidukeConnection.count);

            try {
                try {
                    Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

                    // Sub-Query: we don't want to compare with the entire MDM
                    // This is why we use filters
                    SearchHit[] searchHits;
                    SearchResponse searchResponse;
                    String scrollId;

                    SearchRequest searchRequest = new SearchRequest(indexVal);
                    searchRequest.scroll(scroll);
                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

                    if (!filtersA.isEmpty()) {
                        BoolQueryBuilder bool = new BoolQueryBuilder();
                        String reduceQuery;
                        for (String filter : filtersA) {
                            reduceQuery = "{" +
                                    "           \"match\" : {" +
                                    "               \"" + filter + "\": {" +
                                    "                 \"query\": \"" + ingestMap.get(filter) + "\"" +
                                    "               }" +
                                    "            }" +
                                    "       }";

                            bool.must(new WrapperQueryBuilder(reduceQuery));
                        }

                        searchSourceBuilder.query(bool);
                    }
                    else
                        searchSourceBuilder.query(matchAllQuery());

                    searchRequest.source(searchSourceBuilder);

                    // Search and receive the answer
                    searchResponse = doPrivilegedException(() -> client.search(searchRequest));

                    scrollId = searchResponse.getScrollId();
                    searchHits = searchResponse.getHits().getHits();


                    // Scroll the hits ...
                    while (searchHits != null && searchHits.length > 0) {
                        for (SearchHit sh : searchHits) {
                            // Actions
                            Map<String, ArrayList<Object>> idMap = new HashMap<>();
                            ArrayList<Object> id = new ArrayList<>();
                            id.add(sh.getId());
                            idMap.put("id", id);

                            Collection<MinidukeRecord> records = new ArrayList<>();
                            MinidukeRecord record = new MinidukeRecord();
                            record.setValues(idMap);
                            record.setValues(MinidukeRecord.formatValuesToMap(sh.getSourceAsMap()));
                            records.add(record);

                            Collection<DataSource> arrayESData = new ArrayList<>();
                            arrayESData.add(new MinidukeDatasource(records));

                            properties = Miniduke.checkValuesProperties(sh.getSourceAsMap(), properties);

                            configDuke.setProperties(properties);

                            // Start the engine !
                            no.priv.garshol.duke.Processor dukeProcessor = new no.priv.garshol.duke.Processor(configDuke);

                            // Custom match listener
                            dukeProcessor.addMatchListener(miniDukeML);

                            // ... And link the data
                            dukeProcessor.link(arrayIngestData, arrayESData, 40000);

                        }

                        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                        scrollRequest.scroll(scroll);
                        searchResponse = client.searchScroll(scrollRequest);
                        scrollId = searchResponse.getScrollId();
                        searchHits = searchResponse.getHits().getHits();
                    }

                    // Clear scroll for further requests
                    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                    clearScrollRequest.addScrollId(scrollId);
                    client.clearScroll(clearScrollRequest);
                }
                catch (Exception e) {
                    staticLogger.warn("Scroll error: "+e);
                }


                // Manage the matches
                MinidukeRecord mdr1 = new MinidukeRecord();
                Map<Record, Record> recordsMatches = miniDukeML.getMatches();
                ArrayList<Double> distancesMatches = miniDukeML.getDistance();

                staticLogger.info(miniDukeML.getMatches().size()+" match(es)");

                // Merging the best match to add it to the MDM
                if (miniDukeML.getMatches().size() > 0) {
                    MinidukeRecord mdr2 = new MinidukeRecord();
                    double bestMatch = -1.0;
                    int bestMatchIndex = 0;

                    try {
                        int i=0;
                        for (double distance : distancesMatches) {
                            if (distance > bestMatch) {
                                bestMatch = distance;
                                bestMatchIndex = i;
                            }
                            i++;
                        }

                        i = 0;
                        for (Map.Entry<Record, Record> bestMatchRecords : recordsMatches.entrySet()) {
                            if (i == bestMatchIndex) {
                                mdr1 = (MinidukeRecord) bestMatchRecords.getKey();
                                mdr2 = (MinidukeRecord) bestMatchRecords.getValue();
                            }
                            i++;
                        }

                        // Merge common values
                        mdr1.mergeMiniduke(mdr2);

                        staticLogger.info("Data reconciled !");
                        staticLogger.info("esid: "+mdr1.getESId()+" / d="+bestMatch);

                    }
                    catch (Exception e) {
                        staticLogger.warn("MDM merging error: "+e);
                    }
                }
                else if (miniDukeML.getMaybeMatches().size() > 0) {
                    MinidukeRecord mdr2 = new MinidukeRecord();
                    double bestMatch = -1.0;
                    int bestMatchIndex = 0;

                    try {
                        int i=0;
                        for (double distance : miniDukeML.getMaybeDistance()) {
                            if (distance > bestMatch) {
                                bestMatch = distance;
                                bestMatchIndex = i;
                            }
                            i++;
                        }

                        i = 0;
                        for (Map.Entry<Record, Record> bestMatchRecords : miniDukeML.getMaybeMatches().entrySet()) {
                            if (i == bestMatchIndex) {
                                mdr1 = (MinidukeRecord) bestMatchRecords.getKey();
                                mdr2 = (MinidukeRecord) bestMatchRecords.getValue();
                            }
                            i++;
                        }

                        staticLogger.info("Suspicious data reconciled");
                        staticLogger.info("esid: "+mdr1.getESId()+" / d="+bestMatch);
                        mdr1.makeSuspiciousSource(mdr2);

                        ingestDocument.setFieldValue("_index", "suspicious");
                    }
                    catch (Exception e) {
                        staticLogger.warn("MDM MAYBE merging error: "+e);
                    }
                }
                else {
                    mdr1 = ingestRecord;
                }

                try {
                    if (mdr1 != null) {
                        for (String prop : mdr1.getProperties()) {
                            if ("id".equals(prop)) {
                                String esid = mdr1.getESId();

                                if (!esid.equals("")) {
                                    ingestDocument.setFieldValue("_id", esid);
                                }
                            } else {
                                ingestDocument.setFieldValue(prop, mdr1.getValues(prop));
                            }
                        }
                    }
                }
                catch (Exception e) {
                    staticLogger.warn("---------------------------");
                    staticLogger.warn("Ingest failed: "+e);
                    staticLogger.warn("---------------------------");
                }
            }
            catch (Exception e) {
                staticLogger.warn("---------------------------");
                staticLogger.warn("Procedure execution failed: "+e);
                staticLogger.warn("---------------------------");
            }

        }
        catch (Exception e) {
            staticLogger.error("---------------------------");
            staticLogger.error("Global error: "+e);
            staticLogger.error("The plugin hasn't been used");
            staticLogger.error("---------------------------");
        }
    }


    public static final class MinidukeFactory implements Processor.Factory {
        @Override
        public Processor create(Map<String, Factory> processorFactories, String tag, Map<String, Object> config) {
            String fields = readStringProperty(NAME, tag, config, "fields");
            String threshold = readStringProperty(NAME, tag, config, "threshold");
            String thresholdMaybe = readStringProperty(NAME, tag, config, "thresholdMaybe");
            String weight = readStringProperty(NAME, tag, config, "weight");
            String data = readStringProperty(NAME, tag, config, "data");
            String filters = readStringProperty(NAME, tag, config, "filters");
            String comparator = readStringProperty(NAME, tag, config, "comparator");
            String host = readStringProperty(NAME, tag, config, "host");

            return new MinidukeProcessor(tag, fields, threshold,thresholdMaybe, weight, data, filters, comparator, host);
        }
    }

}
