package io.github.pashazz;

import org.apache.commons.cli.*;

public class Main {
    /*
    For now I configured grafana for myself at port 5000
     */

    public static void showHelp(Options options) {
        var formatter = new HelpFormatter();
        var progName = System.getProperty("sun.java.command").split(" ")[0];
        formatter.printHelp(progName, options);
    }

    public static void main(String[] args) {
	// write your code here
        var options = new Options();
        options.addRequiredOption("x", "xen", true,  "XenServer API (Pool master host) URL");
        options.addRequiredOption("u", "username", true, "XenAPI administrator username");
        options.addRequiredOption("p", "password", true, "XenAPI administrator password");
        options.addOption("db", "influxDB", true, "InfluxDB URL. Default: http://localhost:8086");
        options.addOption("h", "help", false, "print help message");
        options.addOption("d", "debug", false, "write xml files into /tmp/ folder");

        var parser = new DefaultParser();
        CommandLine line;
        try {
            line = parser.parse(options, args);

        }
        catch (ParseException ex) {
            System.err.println("Argument parser failed: " + ex.getLocalizedMessage());
            showHelp(options);
            return;
        }

        if (line.hasOption("h")) {
            showHelp(options);
            return;
        }
        String influxDB;
        if (!line.hasOption("db"))
            influxDB = "http://localhost:8086";
        else
            influxDB = line.getOptionValue("db");

        var masterHostUrl = line.getOptionValue("x");
        var username = line.getOptionValue("u");
        var password = line.getOptionValue("p");


        try {
            Loader loader = new Loader(masterHostUrl, username, password, influxDB, line.hasOption("d"));
            loader.start();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
