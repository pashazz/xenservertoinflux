package io.github.pashazz;

public class Main {
    /*
    For now I configured grafana for myself at port 5000
     */
    public static void main(String[] args) {
	// write your code here
        System.out.println("Hello, World!");
        try {
            Loader loader = new Loader("http://10.10.10.18", "root", "!QAZxsw2", "http://localhost:8086");
            loader.start();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
