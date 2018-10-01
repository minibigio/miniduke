package io.minibig.miniduke.ingest;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.logging.ESLoggerFactory;

/**
 * This class was made to make a nice static connection to avoid
 * creating a new RestClient at each ingestion
 */
public class MinidukeConnection {

    public static RestHighLevelClient client;
    public static int count = 0;

    /**
     * Connects the plugin to an external rest client
     * It is called at each ingestion and is added a counter
     *
     * @param host the address to connect to
     */
    public static void connect(String host) {
        if (count < 1) {
            String[] hostSplit = host.split(":"); // Split host:port into [host, (int)port]
            RestClient rest = RestClient.builder(new HttpHost(hostSplit[0], Integer.parseInt(hostSplit[1]), "http")).build();
            client = new RestHighLevelClient(rest);
        }
        count++;
    }

}
