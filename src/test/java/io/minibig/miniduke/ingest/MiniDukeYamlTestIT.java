package io.minibig.miniduke.ingest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

import java.net.URISyntaxException;

public class MiniDukeYamlTestIT extends ESClientYamlSuiteTestCase {

    public MiniDukeYamlTestIT(@Name("yaml") ClientYamlTestCandidate testCandidate) throws URISyntaxException {
        super(testCandidate);

    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }
}
