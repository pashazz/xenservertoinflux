package io.github.pashazz;

import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.Session;
import com.xensource.xenapi.Types;
import org.apache.xmlrpc.XmlRpcException;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Loader implements Closeable {
    protected Connection xapiConnection;
    protected InfluxDB influxDB;
    protected String dbName = "vmemperor";
    protected String basicAuth;
    protected Instant lastLoad;

    class Legend  {
        private String uuid;
        private String field;
        private String statType; // "AVERAGE", "MIN", "MAX"
        public Legend(String[] legend) {
            statType = legend[0];
            uuid = legend[2];
            field = legend[3];
        }

        public String getUuid() {
            return uuid;
        }

        public String getField() {
            return field;
        }

        public String getStatType() {
            return statType;
        }
    }
    /**
     * Connects to InfluxDB and configures Xen API params
     *
     * @param masterHostUrl URL of xapi pool's master hos
     * @throws MalformedURLException
     */
    public Loader(final String masterHostUrl, final String xapiUsername, final String xapiPassword, final String influxDBUrl
    ) throws MalformedURLException, Types.XenAPIException, XmlRpcException {

        influxDB = InfluxDBFactory.connect(influxDBUrl);
        influxDB.setDatabase(dbName);
        BatchOptions batchOptions = BatchOptions.DEFAULTS;
        influxDB.enableBatch(batchOptions);
        xapiConnection = new Connection(new URL(masterHostUrl));
        Session.loginWithPassword(xapiConnection, xapiUsername, xapiPassword);
        basicAuth = "Basic " + Base64.getEncoder().encodeToString(String.format("%s:%s", xapiUsername, xapiPassword).getBytes());
    }

    public void start() {
        try {
            while (true) {
                getUpdates(5);
                Thread.sleep(2000);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void close() {
        influxDB.close();
        xapiConnection.dispose();
    }


    protected void getUpdates(int secondsAgo) throws IOException, Types.XenAPIException, XmlRpcException {
        if (lastLoad == null)
            lastLoad = Instant.now().minus(Duration.ofSeconds(secondsAgo));
        for (var host : Host.getAll(xapiConnection)) {
            var apiUrl = String.format("http://%s/rrd_updates/?start=%d", host.getAddress(xapiConnection), lastLoad.getEpochSecond());
            try {
                var url = new URL(apiUrl);
                var conn = url.openConnection();

                conn.setRequestProperty("Authorization", basicAuth);
                try(var reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    readXmlData(new InputSource(reader));
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }


            //next: https://jansipke.nl/getting-cpu-memory-disk-and-network-metrics-from-xenserver/ and https://github.com/influxdata/influxdb-java
        }
    }
    protected void readXmlData (InputSource inputSource) throws ParserConfigurationException, IOException, SAXException {
        var domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware(true);
        var builder = domFactory.newDocumentBuilder();
        var doc = builder.parse(inputSource);
        var nodes = doc.getDocumentElement().getChildNodes(); //<xport> {...} </xport>
        Map<String, Point.Builder> legendMap = null;
        var legends = new LinkedList<Legend>();
        for (int i = 0; i < nodes.getLength(); ++i) {
            var node = nodes.item(i);
            if (node.getNodeName().equals("meta")) {
                var metaNodes = node.getChildNodes();
                for (int j = 0; j < metaNodes.getLength(); ++j) {
                    var metaNode = metaNodes.item(i);
                    if (metaNode.getNodeName().equals("end")) {
                        lastLoad = Instant.ofEpochSecond(Long.parseLong(metaNode.getNodeValue()));
                    }
                    else if (metaNode.getNodeName().equals("legend")) {
                        var legendNodes = metaNode.getChildNodes();
                        for (int k = 0; k < legendNodes.getLength(); ++k) {
                            var legendNode = legendNodes.item(k);
                            var arrLegend = legendNode.getNodeValue().split(":");
                            var legend = new Legend(arrLegend);
                            legends.add(legend);
                        }
                        legendMap = initializeLegendMap(legends);
                    }
                }
            }
            else if (node.getNodeName().equals("data")) {
                var rowNodes = node.getChildNodes();
                for (int j = 0; j < rowNodes.getLength(); ++j) {
                    var rowNode = rowNodes.item(j);
                    addDataPointFromDataNode(rowNode, legends, legendMap);
                    clearLegendMap(legendMap);
                }
            }
        }
    }
    protected void addDataPointFromDataNode(Node dataNode, List<Legend> legends, Map<String, Point.Builder> legendMap) {
        var nodes = dataNode.getChildNodes();
        var timeNode = nodes.item(0);
        if (!timeNode.getNodeName().equals("t")) {
            throw new IllegalArgumentException(String.format("timeNode name is %s. Expected: t", timeNode.getNodeName()));
        }

        legendMap.forEach((uuid, point) -> legendMap.replace(uuid, point.time(Long.parseLong(timeNode.getNodeValue()), TimeUnit.SECONDS)));
        var currentNode = timeNode.getNextSibling();
        for (var i = legends.iterator(); i.hasNext() && (currentNode != null);) {
            if (!currentNode.getNodeName().equals("v"))
                throw new IllegalArgumentException(String.format("currentNode name is %s. Expected: v", currentNode.getNodeName()));
            var legend = i.next();
            legendMap.replace(legend.getUuid(),
                    legendMap.get(legend.getUuid()).addField(legend.getField(), Double.parseDouble(currentNode.getNodeValue())));

        }
        buildLegendMap(legendMap);
    }
    protected Map<String, Point.Builder> initializeLegendMap (List<Legend> legends) {
        var map = new HashMap<String, Point.Builder>();
        for (var legend: legends) {
            map.putIfAbsent(legend.getUuid(), Point.measurement(legend.getUuid()));
        }
        return map;
    }

    protected void clearLegendMap(Map<String, Point.Builder> map) {
        for (var s: map.keySet()) {
            map.replace(s, Point.measurement(s));
        }
    }

    protected void buildLegendMap(Map<String, Point.Builder> map) {
        map.values().forEach(point -> influxDB.write(point.build()));
    }
}
