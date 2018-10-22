package io.minibig.miniduke.ingest;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.logging.log4j.Logger;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.WritePipelineResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


import static org.hamcrest.core.Is.is;


//@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class MiniDukeIntegrationTests extends ESIntegTestCase {

    private static Logger staticLogger = ESLoggerFactory.getLogger("MiniDukeIT");
    private int id = 100;

    private String pipelineJson;
    private List<String> fields = new ArrayList<>();
    private List<Object> weight = new ArrayList<>();
    private List<String> filters = new ArrayList<>(); // Default: empty to be sure to not forget any data
    private List<String> comparators = new ArrayList<>();
    private Map<String, Object> testITdata;
    private String indexName = "miniduke_tests";
    private String suspiciousIndexName = "suspicious";

    public MiniDukeIntegrationTests() {
        // Prepare piepline entity
        this.pipelineJson = "{\n" +
                "\"description\":\"miniduke\",\n" +
                "\"processors\":[\n" +
                "{\n" +
                "\"miniduke\":{\n" +
                "\"fields\":\"fields\",\n" +
                "\"threshold\":\"threshold\",\n" +
                "\"thresholdMaybe\":\"thresholdMaybe\",\n" +
                "\"weight\":\"weight\",\n" +
                "\"data\":\"data\",\n" +
                "\"comparator\":\"comparator\",\n" +
                "\"filters\":\"filters\",\n" +
                "\"host\":\"host\"\n" +
                "}\n" +
                "}]\n" +
                "}";


        fields.add("first_name");
        fields.add("last_name");
        fields.add("birth");
        fields.add("gender");
        fields.add("mail");
        fields.add("address1");
        fields.add("address2");
        fields.add("zip");
        fields.add("town");
        fields.add("tel1");
        fields.add("tel2");
        fields.add("country");


        weight.add(new double[]{0.29, 0.68}); // First name
        weight.add(new double[]{0.29, 0.74}); // Last name
        weight.add(new double[]{0.21, 0.55}); // Date of birth
        weight.add(new double[]{0.1, 0.51}); // Gender
        weight.add(new double[]{0.47, 0.81}); // Email
        weight.add(new double[]{0.30, 0.73}); // Address 1
        weight.add(new double[]{0.33, 0.73}); // Address 2
        weight.add("[0.41, 0.58]"); // Zip code
        weight.add("[0.44, 0.55]"); // Town
        weight.add("[0.25, 0.85]"); // Phone 1
        weight.add("[0.35, 0.82]"); // Phone 2
        weight.add("[0.3, 0.57]"); // Country


        comparators.add("no.priv.garshol.duke.comparators.Levenshtein");
        comparators.add("no.priv.garshol.duke.comparators.Levenshtein");
        comparators.add("io.minibig.miniduke.comparators.DateComparator");
        comparators.add("no.priv.garshol.duke.comparators.ExactComparator");
        comparators.add("io.minibig.miniduke.comparators.EmailComparator");
        comparators.add("io.minibig.miniduke.comparators.AddressComparator");
        comparators.add("io.minibig.miniduke.comparators.AddressComparator");
        comparators.add("io.minibig.miniduke.comparators.ZipComparator");
        comparators.add("no.priv.garshol.duke.comparators.WeightedLevenshtein");
        comparators.add("io.minibig.miniduke.comparators.PhoneComparator");
        comparators.add("io.minibig.miniduke.comparators.PhoneComparator");
        comparators.add("no.priv.garshol.duke.comparators.ExactComparator");

    }

    private Map<String, Object> reset(List<String> data) {
        testITdata = new HashMap<>();
        testITdata.put("fields", fields);
        testITdata.put("threshold", "0.95");
        testITdata.put("thresholdMaybe", "0.88");
        testITdata.put("weight", weight);
        testITdata.put("data", data);
        testITdata.put("filters", filters);
        testITdata.put("comparator", comparators);
        testITdata.put("host", "localhost:9200");
        return testITdata;
    }

    public Map<String, Object> entityAsMap(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (XContentParser parser = createParser(xContentType.xContent(), response.getEntity().getContent())) {
            return parser.map();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(IngestMinidukePlugin.class);
    }

    public void testPluginIsLoaded() {
        staticLogger.info("Test plugin loaded ?");
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setPlugins(true).get();
        boolean pluginFound = false;

        for (NodeInfo nodeInfo : response.getNodes()) {
            pluginFound = false;
            for (PluginInfo pluginInfo : nodeInfo.getPlugins().getPluginInfos()) {
                if (pluginInfo.getName().equals(IngestMinidukePlugin.class.getName())) {
                    pluginFound = true;
                    break;
                }
            }

        }

        assertThat(pluginFound, is(true));
        staticLogger.info("Yes! Great, lets continue :)");
    }

    // Jarvis Pepperspray
    public void testInsertJarvis() throws Exception {
        staticLogger.info("Hi, this is InsertJarvis test. Let's go !");

        ////For6.x
        //ByteBufferbuf=ByteBuffer.allocate(100);
        //CharBuffercbuf=buf.asCharBuffer();
        //cbuf.put(json);
        //cbuf.flip();

        //client().admin().cluster().putPipeline(new PutPipelineRequest("miniduke",newByteBufferReference(buf)));

        // For 5.6
        ActionFuture<WritePipelineResponse> r =
                client().admin().cluster().putPipeline(new PutPipelineRequest("miniduke", new BytesArray(pipelineJson), XContentType.JSON));
        staticLogger.info(r.actionGet().isAcknowledged());
        client().admin().indices().create(new CreateIndexRequest(this.indexName));
        staticLogger.info("Ready to test !");

        List<String> data = new ArrayList<>();
        data.add("Jarvis");
        data.add("Pepperspray");
        data.add("1970-02-18");
        data.add("1");
        data.add("jarvispeppespray@johndoe.me");
        data.add("56 rue Beauvau");
        data.add("");
        data.add("13003");
        data.add("Marseille");
        data.add("");
        data.add("[0486019101,0684215191]");
        data.add("France");

        filters = new ArrayList<>();


        Map<String, Object> testITdata = reset(data);
        ActionFuture<IndexResponse> indexResp;

        staticLogger.info("> Data ready for: Jarvis");
        indexResp = client().index(new IndexRequest(this.indexName, "mdm", "" + this.id).source(testITdata).setPipeline("miniduke"));
        staticLogger.info("> Data sent on ESIntegTests");

        staticLogger.info("Status: "+indexResp.actionGet().status());

        // Wait for the action to be finished
        TimeUnit.SECONDS.sleep(1);

        // _id=41 because the data to reconcile is 41
        GetResponse req = client().get(new GetRequest(this.indexName, "mdm", "11")).get();
        Map<String, Object> res = req.getSourceAsMap();

        ArrayList<Object> expectedAddress = new ArrayList<>();
        expectedAddress.add("56 rue Beauvau");

        assertEquals(expectedAddress, res.get("address1"));
    }

    // Agathe P Royer
    public void testInsertAgathe() throws Exception {
        staticLogger.info("Hi, this is InsertAgathe test. Let's go !");

        //client().admin().cluster().putPipeline(new PutPipelineRequest("miniduke", new BytesArray(pipelineJson), XContentType.JSON));
        ActionFuture<WritePipelineResponse> r =
                client().admin().cluster().putPipeline(new PutPipelineRequest("miniduke", new BytesArray(pipelineJson), XContentType.JSON));
        staticLogger.info(r.actionGet().isAcknowledged());
        client().admin().indices().create(new CreateIndexRequest(this.indexName));
        staticLogger.info("Index [miniduke_tests] created on ESIntegTests");

        List<String> data = new ArrayList<>();
        data.add("Agathe");
        data.add("Royer");
        data.add("1969-03-12");
        data.add("0");
        data.add("aroyer@johndoe.org");
        data.add("128 boulevard de Prague");
        data.add("");
        data.add("79000");
        data.add("");
        data.add("0628322809");
        data.add("0123456789");
        data.add("");


        filters = new ArrayList<>();
        filters.add("last_name");

        Map<String, Object> testITdata = reset(data);
        ActionFuture<IndexResponse> indexResp;

        staticLogger.info("> Data ready for: Agathe");
        indexResp = client().index(new IndexRequest(this.indexName, "mdm", "" + this.id).source(testITdata).setPipeline("miniduke"));
        staticLogger.info("> Data sent on ESIntegTests");

        staticLogger.info("Status: "+indexResp.actionGet().status());

        // Wait for the action to be finished
        TimeUnit.SECONDS.sleep(1);

        GetResponse req = client().get(new GetRequest(this.indexName, "mdm", "21")).get();
        Map<String, Object> res = req.getSourceAsMap();

        ArrayList<Object> expectedFirstName = new ArrayList<>();
        expectedFirstName.add("Agathe");

        assertEquals(expectedFirstName, res.get("first_name"));


        this.id++;

        // Wait for the action to be finished
        TimeUnit.SECONDS.sleep(2);

        filters = new ArrayList<>();

        data = new ArrayList<>();
        data.add("Agatthe");
        data.add("Roier");
        data.add("1969-03-12");
        data.add("0");
        data.add("aroyer@johndoe.org");
        data.add("128 boulevard de PRAGUE");
        data.add("");
        data.add("75000");
        data.add("0628322809");
        data.add("0635248955");
        data.add("");
        data.add("");


        testITdata = reset(data);

        staticLogger.info("> Data ready for: Agatthe");
        client().index(new IndexRequest(this.indexName, "mdm", "" + this.id).source(testITdata).setPipeline("miniduke"));
        staticLogger.info("> Data sent on ESIntegTests");

        staticLogger.info("Status: "+indexResp.actionGet().status());

        // Wait for the action to be finished
        TimeUnit.SECONDS.sleep(1);

        GetResponse req2 = client().get(new GetRequest(this.indexName, "mdm", "21")).get();
        Map<String, Object> res2 = req2.getSourceAsMap();

        ArrayList<String> expectedFirstName2 = new ArrayList<>();
        expectedFirstName2.add("Agathe");
        expectedFirstName2.add("Agatthe");

        assertEquals(expectedFirstName2, res2.get("first_name"));

        ArrayList<String> expectedGender = new ArrayList<>();
        expectedGender.add("0");
        assertEquals(expectedGender, res2.get("gender"));
    }


    // Mae M Walton
    public void testInsertMae() throws Exception {
        staticLogger.info("Hi, this is InsertMae test. Let's go!");

        // For 5.6
        ActionFuture<WritePipelineResponse> r =
                client().admin().cluster().putPipeline(new PutPipelineRequest("miniduke", new BytesArray(pipelineJson), XContentType.JSON));
        staticLogger.info(r.actionGet().isAcknowledged());
        client().admin().indices().create(new CreateIndexRequest(this.indexName));
        staticLogger.info("Index [miniduke_tests] created on ESIntegTests");

        List<String> data = new ArrayList<>();
        data.add("Mae");
        data.add("Walon");
        data.add("1973-07-22");
        data.add("0");
        data.add("hulda73@yahoo.com");
        data.add("4970 Avenue Saint-Marys");
        data.add("");
        data.add("75012");
        data.add("");
        data.add("");
        data.add("0646568956");
        data.add("");
//Mae M Walton
        filters = new ArrayList<>();

        Map<String, Object> testITdata = reset(data);
        ActionFuture<IndexResponse> indexResp;

        staticLogger.info("> Data ready for: Mae");
        indexResp = client().index(new IndexRequest(this.indexName, "mdm", "" + this.id).source(testITdata).setPipeline("miniduke"));
        staticLogger.info("> Data sent on ESIntegTests");

        staticLogger.info("Status: "+indexResp.actionGet().status());

        // Wait for the action to be finished
        TimeUnit.SECONDS.sleep(1);

        GetResponse req = client().get(new GetRequest(this.indexName, "mdm", "31")).get();
        Map<String, Object> res = req.getSourceAsMap();

        ArrayList<Object> expectedLastName = new ArrayList<>();
        expectedLastName.add("Walton");
        expectedLastName.add("Walon");

        assertEquals(expectedLastName, res.get("last_name"));

        this.id++;

        // Wait for the action to be finished
        TimeUnit.SECONDS.sleep(2);

        data = new ArrayList<>();
        data.add("Mae");
        data.add("Walton");
        data.add("1973-07-22");
        data.add("0");
        data.add("hulda1973@yahoo.com");
        data.add("4970 Avenue Saint Marys");
        data.add("");
        data.add("75012");
        data.add("");
        data.add("");
        data.add("0646568956");
        data.add("");

        testITdata = reset(data);

        staticLogger.info("> Data ready for: Mae");
        client().index(new IndexRequest(this.indexName, "mdm", "" + this.id).source(testITdata).setPipeline("miniduke"));
        staticLogger.info("> Data sent on ESIntegTests");

        // Wait for the action to be finished
        TimeUnit.SECONDS.sleep(1);

        GetResponse req2 = client().get(new GetRequest(this.indexName, "mdm", "31")).get();
        Map<String, Object> res2 = req2.getSourceAsMap();

        ArrayList<String> expectedFirstName2 = new ArrayList<>();
        expectedFirstName2.add("Mae");

        assertEquals(expectedFirstName2, res2.get("first_name"));
    }

    @SuppressWarnings("unchecked")
    public void testInsertMaybeMae() throws Exception {
        staticLogger.info("Hi, this is InsertMaybeMae test. Let's go!");

        // For 5.6
        ActionFuture<WritePipelineResponse> r =
                client().admin().cluster().putPipeline(new PutPipelineRequest("miniduke", new BytesArray(pipelineJson), XContentType.JSON));
        staticLogger.info(r.actionGet().isAcknowledged());
        client().admin().indices().create(new CreateIndexRequest(this.indexName));
        client().admin().indices().create(new CreateIndexRequest(this.suspiciousIndexName));
        staticLogger.info("Index [miniduke_tests] created on ESIntegTests");

        List<String> data = new ArrayList<>();
        data.add("Maé");
        data.add("Wallon");
        data.add("1972-07-19");
        data.add("0");
        data.add("hulda73@yahoo.com");
        data.add("497 Avn Saint-Marys");
        data.add("");
        data.add("75012");
        data.add("");
        data.add("");
        data.add("0698295637");
        data.add("");

        HashMap<String, Object> keyValues = new HashMap<>();
        ArrayList<String> tmp;
        for (int i=0; i<this.fields.size(); i++) {
            tmp = new ArrayList<>();
            tmp.add(data.get(i));
            keyValues.put(this.fields.get(i), tmp);
        }
        tmp = new ArrayList<>();
        tmp.add("ingest");
        keyValues.put("id", tmp);

        filters = new ArrayList<>();

        Map<String, Object> testITdata = reset(data);
        ActionFuture<IndexResponse> indexResp;

        staticLogger.info("> Data ready for: Mae");
        indexResp = client().index(new IndexRequest(this.indexName, "mdm", "" + this.id).source(testITdata).setPipeline("miniduke"));
        staticLogger.info("> Data sent on ESIntegTests");

        staticLogger.info("Status: "+indexResp.actionGet().status());

        // Wait for the action to be finished
        TimeUnit.SECONDS.sleep(1);

        GetResponse req = client().get(new GetRequest(this.suspiciousIndexName, "mdm", ""+this.id)).get();
        Map<String, Object> res = req.getSourceAsMap();

        boolean equals = true;
        // FIXME : Warning unchecked cast, can't manage to find a nice way without SuppressWarnings
        Map<String, Object> res2 = (Map<String, Object>) res.get("2");

        for (String field : this.fields) {
            if (keyValues.containsKey(field) && res2.containsKey(field)) {
            boolean equalLists = keyValues.get(field).toString().contentEquals(res2.get(field).toString())?true:false;
                if (!equalLists)
                    equals = false;
            }
        }

        assertTrue(equals);
    }

}
