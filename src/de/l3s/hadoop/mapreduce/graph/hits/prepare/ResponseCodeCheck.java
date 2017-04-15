package de.l3s.hadoop.mapreduce.graph.hits.prepare;

import java.net.HttpURLConnection;
import java.net.URL;

// Just for testing

public class ResponseCodeCheck 
{

    public static void main (String args[]) throws Exception
    {
    	String return_url = "http://www.tagesschau.de/thema/merkel/index.htmlgdfgdfg";

        URL url = new URL("https://web.archive.org/web/*/" + return_url);
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();

        int code = connection.getResponseCode();
        System.out.println("Response code of the object is "+code);
        if (code==200)
        {
            System.out.println("OK");
        }
    }
}