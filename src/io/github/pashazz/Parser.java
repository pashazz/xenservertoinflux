package io.github.pashazz;

import org.influxdb.dto.Point;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Parser {
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

    private Loader loader;
    private InputSource inputSource;
    private Node metaNode;
    private Node dataNode;
    Map<String, Point.Builder> legendMap;
    List<String> legends;

    public Parser(Loader loader, InputSource inputSource) {
        this.loader = loader;
        this.inputSource = inputSource;
    }
    public void parse() {
        try {
            var domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setNamespaceAware(true);
            var builder = domFactory.newDocumentBuilder();
            var doc = builder.parse(inputSource);
            var nodes = doc.getDocumentElement().getChildNodes(); //<xport> {...} </xport>
            for (int i = 0; i < nodes.getLength(); ++i) {
                var node = nodes.item(i);
                if (node.getNodeName().equals("meta")) {
                    metaNode = node;
                } else if (node.getNodeName().equals("data")) {

                }
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    private void processDataNode() {
        var rowNodes = node.getChildNodes();
        for (int j = 0; j < rowNodes.getLength(); ++j) {
            var rowNode = rowNodes.item(j);
            addDataPointFromDataNode(rowNode, legends, legendMap);
            clearLegendMap(legendMap);
        }
    }
}

