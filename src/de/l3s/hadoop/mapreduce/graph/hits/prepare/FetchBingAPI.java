package de.l3s.hadoop.mapreduce.graph.hits.prepare;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;

// Prepare datasets:
// step1: Fetch root set from Bing (this program)
// step2: Convert Bing result to SURT form
// step3: Build base set from map/reduce 

public class FetchBingAPI {

	public static final String storePath = "Z:/Alexandria/Results/Result_follow_up_WebSci16/Live_Bing_Result_Fetching/";
	public static final String entity_Query = "Barack Obama";
	public static final int MAX_RESULT = 10000;

	public static void main(final String[] args) throws Exception {
		int skip = 0;
		final String accountKey = "p6b1XscNBx8tLcd6ht/RlTtCOJ5ZA6H4rGJ5xB+Ze4s";

		String bingUrlPattern;
		String query;

		String accountKeyEnc = Base64.getEncoder().encodeToString(
				(accountKey + ":" + accountKey).getBytes());

		File result_out = new File(storePath + entity_Query.replace(' ', '_'));
		BufferedWriter write_result = new BufferedWriter(new FileWriter(
				result_out));

		String bingUrl;
		URL url;
		URLConnection connection;

		for (skip = 0; skip < MAX_RESULT; skip += 50) {

			bingUrlPattern = "https://api.datamarket.azure.com/Bing/Search/Web?Query=%%27%s%%27&$skip="
					+ skip + "&$format=JSON";
			query = URLEncoder.encode("'" + entity_Query + "'", Charset
					.defaultCharset().name());
			bingUrl = String.format(bingUrlPattern, query);
			url = new URL(bingUrl);
			connection = url.openConnection();
			connection.setRequestProperty("Authorization", "Basic "
					+ accountKeyEnc);

			BufferedReader in = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));
			String inputLine;
			final StringBuilder response = new StringBuilder();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			final JSONObject json = new JSONObject(response.toString());
			final JSONObject d = json.getJSONObject("d");
			final JSONArray results = d.getJSONArray("results");
			final int resultsLength = results.length();
			for (int i = 0; i < resultsLength; i++) {
				final JSONObject aResult = results.getJSONObject(i);
				String return_url = aResult.get("Url").toString();
				//URL archive_url = new URL("https://web.archive.org/web/*/"
				//		+ return_url);
				//HttpURLConnection http_connect = (HttpURLConnection) archive_url
				//		.openConnection();
				//http_connect.setRequestMethod("GET");

				//int status_code = 0;

				//try {
				//	http_connect.connect();
				//	status_code = http_connect.getResponseCode();
				//} catch (Exception e) {

				//	System.out.println(e.getMessage() + return_url);
				//}

				//if (status_code == 200) {
				//	write_result.write(return_url + "\r\n");
				//}
				
				write_result.write(return_url + "\r\n");
			}

		}

		write_result.close();
	}

}