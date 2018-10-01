package io.minibig.miniduke.ingest;

import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.Map;

public class IngestMinidukePlugin extends Plugin implements IngestPlugin {

    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap("miniduke", new MinidukeProcessor.MinidukeFactory());
    }
}
