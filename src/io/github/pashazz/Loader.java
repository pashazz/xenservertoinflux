package io.github.pashazz;

import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.Session;
import com.xensource.xenapi.Types;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClientException;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.stream.Collectors;

public class Loader implements Closeable {
    protected Connection xapiConnection;
    protected InfluxDB influxDB;
    protected String dbName = "vmemperor";
    protected String basicAuth;
    /**
     *  Connects to InfluxDB and configures Xen API params
     * @param masterHostUrl URL of xapi pool's master hos
     * @throws MalformedURLException
     */
    public Loader(final String masterHostUrl, final String xapiUsername, final String  xapiPassword, final String influxDBUrl
    )  throws MalformedURLException, Types.XenAPIException, XmlRpcException {

        influxDB = InfluxDBFactory.connect(influxDBUrl);
        influxDB.setDatabase(dbName);
        BatchOptions batchOptions = BatchOptions.DEFAULTS;
        influxDB.enableBatch(batchOptions);

        xapiConnection = new Connection(new URL( masterHostUrl));
        Session.loginWithPassword(xapiConnection, xapiUsername, xapiPassword);
        basicAuth = "Basic " + Base64.getEncoder().encodeToString(String.format("%s:%s", xapiUsername, xapiPassword).getBytes());
    }

    public void start() {

    }

    public void close() {
        influxDB.close();
        xapiConnection.dispose();
    }



    protected void getUpdates(int secondsAgo) throws IOException, Types.XenAPIException, XmlRpcException {
        var instant = Instant.now().minus(Duration.ofSeconds(secondsAgo));
        for (var host : Host.getAll(xapiConnection)) {
            var apiUrl = String.format("http://%s/rrd_updates/?start=%d", host.getAddress(xapiConnection), instant.getEpochSecond());
            try {
                var url = new URL(apiUrl);
                var conn = url.openConnection();

                conn.setRequestProperty("Authorization", basicAuth);
                var data = new BufferedReader(new InputStreamReader(conn.getInputStream()))
                        .lines().parallel().collect(Collectors.joining("\n"));

            }
            catch (Exception ex)  {
                System.out.println(ex.toString());
        }

            //next: https://jansipke.nl/getting-cpu-memory-disk-and-network-metrics-from-xenserver/ and https://github.com/influxdata/influxdb-java
    }
}
