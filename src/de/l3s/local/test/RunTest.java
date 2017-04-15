package de.l3s.local.test;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URLDecoder;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Period;
import org.apache.commons.lang.StringEscapeUtils;
import org.elasticsearch.common.lang3.StringUtils;

import com.entopix.maui.stemmers.GermanStemmer;
import com.entopix.maui.stopwords.StopwordsGerman;

class RankingSort implements Comparator<Long> {

	public int compare(Long one, Long two) {
		if (one < two) {
			return -1;
		} else {
			if (one > two)
				return 1;
			else
				return 0;
		}
	}
}

public class RunTest {
	public static final int MAX_LENGTH = 2147483646;

	public static String getReverse(String dest_url) {
		String decodeURL = dest_url, revfirst;

		if (decodeURL.indexOf("www2.") == 0)
			decodeURL = decodeURL.replaceFirst(Pattern.quote("www2."), "");

		if (decodeURL.indexOf("www.") == 0)
			decodeURL = decodeURL.replaceFirst(Pattern.quote("www."), "");

		String[] dn = decodeURL.split(Pattern.quote("."));
		int len = dn.length;
		revfirst = dn[0];
		for (int i = 1; i < len; i++) {
			revfirst = dn[i] + "," + revfirst;

		}
		return (revfirst + ")").toLowerCase();
	}

	public static void merge_anchor() {
		String anchors = "", currentAnchor = "";
		TObjectIntHashMap<String> anchor_list = new TObjectIntHashMap<String>();
		int len = 0;
		int inlink = 0;
		String[] raw = { "wyss samen und pflanzen", "wyss samen und pflanzen",
				"wyss samen und pflanzen", "wyss samen und pflanzen",
				"wyss samen und pflanzen", "wyss samen und pflanzen",
				"wyss samen und pflanzen", "wyss samen und pflanzen",
				"wyss samen und pflanzen", "wyss samen und pflanzen",
				"wyss samen und pflanzen", "wyss samen und pflanzen",
				"wyss samen und pflanzen", "wyss samen und pflanzen",
				"wyss samen und pflanzen", "wyss samen und pflanzen",
				"wyss samen und pflanzen", };
		for (int i = 0; i < 17; i++) {
			currentAnchor = raw[i];
			inlink++;
			if ((len = anchor_list.get(currentAnchor)) < MAX_LENGTH) {
				System.out.println("Before adjust: " + len);
				anchor_list.adjustOrPutValue(currentAnchor, 1, 1);
				System.out.println("After adjust: "
						+ anchor_list.get(currentAnchor));
				if (anchors.length() + currentAnchor.length() < 1000000)
					anchors += (currentAnchor + " ");
			}
		}
		System.out.println("Merged: " + anchors);
	}

	public static String getDest_url_archived(String dest_url) {
		String decodeURL = dest_url;
		String first = "", later = "", revfirst = "";
		int split_pos = decodeURL.indexOf('/');

		if (split_pos < 0) {
			split_pos = decodeURL.length();
		}
		first = decodeURL.substring(0, split_pos);

		if (split_pos < decodeURL.length())
			later = decodeURL.substring(split_pos + 1);

		if (split_pos >= 4) {

			String[] dn = first.split(Pattern.quote("."));
			int len = dn.length;
			revfirst = dn[0];
			for (int i = 1; i < len; i++) {
				revfirst = dn[i] + "," + revfirst;
			}

		}
		return (revfirst + ")/" + later).toLowerCase();
	}

	public static void loadList(String path, HashSet<String> list) {
		File f = new File(path);
		try {
			BufferedReader bnr = new BufferedReader(new FileReader(f));
			String rl = "";
			while ((rl = bnr.readLine()) != null) {
				list.add(rl);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getArchivedURL(String dest_url) {
		String decodeURL = dest_url, revfirst;

		if (decodeURL.indexOf("www2.") == 0)
			decodeURL = decodeURL.replaceFirst(Pattern.quote("www2."), "");

		if (decodeURL.indexOf("www.") == 0)
			decodeURL = decodeURL.replaceFirst(Pattern.quote("www."), "");

		String[] dn = decodeURL.split(Pattern.quote("."));
		int len = dn.length;
		revfirst = dn[0];
		for (int i = 1; i < len; i++) {
			revfirst = dn[i] + "," + revfirst;

		}
		return (revfirst + ")").toLowerCase();
	}

	public static void main(String agrs[]) {
		
		String test = "http://angela-merkel.de/index.htm http://angela-merkel.de/index.htm http://angela-merkel.de/page/102_851.htm http://angela-merkel.de/index.htm http://angela-merkel.de/page/102_852.htm http://angela-merkel.de/page/102_852.htm http://angela-merkel.de/page/126.htm http://angela-merkel.de/page/102.htm http://angela-merkel.de/page/102.htm http://angela-merkel.de/page/102_853.htm http://angela-merkel.de/page/102_856.htm http://angela-merkel.de/page/135.htm http://angela-merkel.de/page/144.htm http://angela-merkel.de/page/152.htm http://cdu.de/ http://angela-merkel.de/page/102_851.htm http://angela-merkel.de/page/102.htm http://angela-merkel.de/page/102_868.htm http://angela-merkel.de/page/102_853.htm http://angela-merkel.de/page/135.htm http://angela-merkel.de/page/102_856.htm http://angela-merkel.de/page/145.htm http://angela-merkel.de/page/126.htm http://angela-merkel.de/page/152.htm http://angela-merkel.de/page/142.htm http://bundeskanzlerin.de/ http://angela-merkel.de/page/145.htm http://cdu.de/ http://bundeskanzlerin.de/ http://spenden.cdu.de/page/60.htm http://spenden.cdu.de/page/60.htm http://team2009.de/ http://team2009.de/ http://angela-merkel.de/page/135.htm http://angela-merkel.de/page/102_868.htm http://angela-merkel.de/page/142.htm http://angela-merkel.de/page/102_869.htm http://angela-merkel.de/page/102_869.htm http://angela-merkel.de/page/126.htm http://angela-merkel.de/page/144.htm http://angela-merkel.de/page/142.htm http://angela-merkel.de/page/144.htm http://angela-merkel.de/page/145.htm http://angela-merkel.de/page/152.htm http://bundeskanzlerin.de/ http://cdu.de/ http://spenden.cdu.de/page/60.htm http://team2009.de/";
		String line[] = test.split(" ");
		PriorityQueue<String> list = new PriorityQueue<String>();
		for (int i = 0; i < line.length; i ++) {
			if (!list.contains(line[i]))
				list.add(line[i]);
		}
		while (list.size() > 0) {
			System.out.println(list.poll());
		}
		
		
		
//		HashSet<String> wikipediaURLList = new HashSet<String>();
//
//		HashSet<String> wikipediaURLList_Processed = new HashSet<String>();
//
//		String wikipedia_domain_path = "Z:/Alexandria/Data_Input/references-wiki";
//		loadList(wikipedia_domain_path, wikipediaURLList);
//		for (String s : wikipediaURLList) {
//			wikipediaURLList_Processed.add(getArchivedURL(s));
//		}
//
//		for (String s : wikipediaURLList_Processed) {
//			System.out.println(s);
//		}
		
		//DateTime d_test = new DateTime("2017-03-14T16:13:33+01:00");
		//DateTime d_test1 = new DateTime("2013-03-28T20:47:03.000-07:00");
		//System.out.println("Time differ: " + Days.daysBetween(d_test, d_test1).getDays());

		// String token = new
		// String("de,marktplatz-mittelstand)/bresegard-bei-picher/entsorgung");
		// System.out.println(token.matches("de,([^/)]+/\\)*)"));
//		String src = "de,ebay,search)/parma_w0qqcatrefzc12qqcoactionzcompareqqcoentrypagezsearchqqcopagenumz1qqfcclz1qqfclz3qqfromzr2qqfrppz50qqfsooz2qqfsopz2qqsbrexpzwd4sqqsspagenamezwd4s";
//
//		int domain_position = src.indexOf(")");
//		String domain = src.substring(0, domain_position);
//		System.out.println("Domain trimmmed: " + domain);
//
//		Character src_val = new Character('e');
//		Character src_val1 = new Character('E');
//		String year = "2006-06-18T20:24:48.000-07:00";
//		String year1 = "2013-06-18T20:24:48.000-07:00";
//
//		System.out.println("Date: " + year.substring(0, 10));
//		System.out.println("Compare" + year1.compareTo(year));
//
//		String time_series = "2012-09-13 2012-03-23 2012-05-05 2013-07-22 2012-03-08 2013-07-22 2012-03-12 2012-04-01 2012-03-10 2012-03-30 2012-03-18 2012-03-30 2012-03-27 2012-03-25 2012-03-20 2012-03-11 2012-03-12 2012-03-13 2013-07-22 2012-03-08 2012-03-10 2012-03-30 2012-03-25 2012-05-05 2012-04-26 2012-02-27 2012-02-27 2012-03-13 2012-03-12 2012-03-23 2012-03-27 2012-03-30 2012-03-23 2012-03-30 2012-03-10 2012-05-05 2012-03-27 2012-03-27 2012-03-12 2012-03-23 2012-03-23 2012-03-23 2012-04-01 2012-03-09 2012-03-10 2012-03-08 2012-03-23 2012-03-27 2012-03-27 2012-03-25 2012-03-18 2012-03-10 2012-03-09 2012-03-11 2012-09-13 2012-03-30 2012-03-30 2012-03-25 2012-03-23 2012-03-11 2012-03-18 2012-03-11 2012-03-08 2012-03-20 2012-09-12 2012-05-05 2012-09-13 2012-02-13 2012-03-18 2012-05-10 2012-05-05 2012-03-25 2012-04-01 2012-04-12 2012-03-10 2012-04-01 2012-03-23 2012-03-08 2012-04-01 2012-03-25 2012-03-12 2012-03-12 2012-03-13 2012-03-09 2012-03-10 2013-07-22 2012-03-27 2012-03-27 2012-03-30 2012-02-28 2012-03-09 2012-05-05 2012-03-23 2012-03-10 2012-03-23 2012-03-23 2012-03-13 2012-03-11 2012-03-11 2012-03-18 2012-03-12 2012-04-27 2012-03-13 2012-03-27 2013-07-22 2012-04-01 2012-03-10 2012-03-11 2012-03-12 2012-03-30 2012-03-11 2012-03-26 2012-09-13 2012-03-25 2012-03-11 2012-03-25 2012-03-11 2013-07-21 2013-07-21 2012-03-25 2012-03-11 2012-03-26 2012-03-10 2012-03-08 2012-03-27 2012-03-08 2012-05-05 2012-04-01 2012-03-09 2012-04-01 2012-03-27 2012-09-13 2012-03-20 2012-03-27 2012-04-01 2012-04-01 2012-03-27 2012-03-30 2012-03-30 2012-05-05 2012-03-30 2012-04-01 2012-03-22 2012-03-09 2012-03-20 2012-04-01 2013-07-22 2012-03-12 2012-03-11 2012-09-13 2012-03-10 2012-03-30 2012-03-27 2012-04-01 2012-03-08 2012-03-11 2012-03-11 2012-03-10 2012-03-08 2012-03-10 2012-03-30 2012-03-25 2012-03-10 2012-03-08 2012-03-09 2012-03-18 2012-03-25 2012-04-01 2012-03-25 2012-03-08 2012-03-09 2012-03-25 2012-09-13 2012-05-05 2012-05-05 2012-05-05 2012-09-13 2012-03-18 2012-03-20 2013-07-21 2012-03-12 2013-07-22 2012-03-12 2012-03-23 2012-03-13 2012-03-23 2012-03-08 2012-03-12 2012-03-18 2012-03-13 2012-03-25 2012-03-12 2012-09-23 2012-03-12 2012-03-08 2012-03-13 2012-03-08 2012-05-05 2013-07-22 2012-03-18 2012-03-26 2012-03-09 2012-05-05 2012-04-23 2012-03-13 2012-03-20 2012-03-22 2013-07-21 2012-03-13 2012-09-13 2012-09-13 2012-09-12 2012-05-05 2012-02-20 2012-03-13 2012-03-20 2012-03-18 2012-03-22 2012-09-13 2012-03-20 2012-03-18 2012-03-20 2013-07-22 2013-07-22 2012-03-18 2012-03-26 2012-03-13 2013-07-22 2012-03-09 2012-03-20 2012-03-26 2012-03-18 2012-03-26 2013-07-21 2012-03-15 2012-03-13 2012-03-13 2012-07-20 2012-08-23 2012-09-13 2012-09-12 2012-02-13 2012-04-12 2012-02-25 2012-04-26 2012-03-18 2012-03-26 2012-03-20 2012-05-25 2012-09-13 2012-09-12 2012-04-16 2012-09-13 2012-06-06 2012-02-13 2012-03-20 2013-07-22 2012-03-09 2013-07-22 2013-07-21 2012-03-09 2012-03-09 2012-02-11 2012-02-12 2012-03-09 2013-07-21 2012-03-22 2012-03-20 2012-03-20 2013-07-21 2013-07-22 2013-07-21 2013-07-21 2012-04-03 2013-07-21 2012-03-26 2013-07-21 2012-03-29 2012-03-24 2012-03-26 2013-07-21 2012-03-26 2012-03-26 2012-03-22 2012-03-26 2012-09-09 2012-04-29 2012-05-04 2012-10-19 2012-04-14 2012-06-14 2012-09-12 2012-04-14 2012-02-23 2012-06-29 2012-02-12 2012-02-26 2012-02-25 2012-04-19 2012-04-12 2012-04-21 2012-06-05 2012-09-12 2012-05-25 2012-02-28 2012-04-15 2012-03-15 2012-03-15 2012-02-26 2012-05-21 2012-07-06 2012-02-23 2012-09-12 2012-04-27 2012-04-27 2012-06-06 2012-03-26 2012-09-12 2012-09-12 2012-02-09 2012-05-13 2012-03-26 2012-07-07 2012-05-10 2012-02-09 2012-04-14 2012-03-15 2012-04-14 2012-05-24 2012-03-15 2012-06-21 2013-07-21";
//
//		String[] time_part = year.split("[-T:.]");
//
//		String year_val = "2006";
		// System.out.println(src.charAt(3));
		// System.out.println(((src.charAt(3) == src_val.charValue()) ||
		// (src.charAt(3) == src_val1.charValue())) && (year.substring(0,
		// 4).equals(year_val)));
		// System.out.println(ExtractDomain("de,marktplatz-mittelstand)/bresegard-bei-picher/entsorgung"));
//		String test = "britishpathe.com.de/video/new-freighter-acadia-forest";
//		System.out.println("Archive URL: - "
//				+ getDest_url_archived("www.netzeitung.de"));
//		System.out.println("Index: " + test.indexOf("http://www."));
//		test = test.replaceFirst(Pattern.quote("http://www."), "");
//		System.out.println("Replaced: " + test);
//
//		String str1 = "2010-10-27T05:33:43.000-07:00";
//
//		String str2 = "2010-10-27T05:33:43.000-07:00|2010-10-27T05:33:43.000-07:00|2010-10-27T05:33:43.000-07:00|2010-10-27T05:33:43.000-07:00|2010-10-27T05:33:43.000-07:00|";
//		
//		String vector = "stillbuch	0.11090589314699173 -0.2454976737499237 -0.3867828845977783 0.2039690762758255 -0.043397851288318634 -0.017309654504060745 0.016526496037840843 -0.1089511513710022 0.24175329506397247 0.20411136746406555 0.21964097023010254 -0.3068414032459259 -0.36473390460014343 -0.14644451439380646 -0.10661237686872482 -0.04838382080197334 -0.19477377831935883 -0.16510967910289764 -0.08475415408611298 0.05571361258625984 -0.29273465275764465 0.060417644679546356 0.1934964805841446 -0.706344723701477 -0.19894041121006012 0.19499821960926056 -0.13215121626853943 0.053515803068876266 0.06130072847008705 -0.12662768363952637 0.22919966280460358 -0.3277791142463684 -0.401200532913208 0.08177611231803894 0.11770431697368622 -0.24397021532058716 0.19044741988182068 0.23799827694892883 -0.02721736766397953 0.19478394091129303 -0.37620019912719727 -0.5033074021339417 -0.2822866439819336 0.2121746689081192 -0.2136213183403015 0.5907761454582214 0.03138946741819382 -0.24341179430484772 0.03474702313542366 -0.1006292775273323 -0.10713762044906616 -0.36452367901802063 0.2618897259235382 0.043357547372579575 -0.3570220172405243 -0.05915839597582817 -0.15396186709403992 -0.05641289800405502 -0.503315806388855 -0.14415328204631805 -0.32208681106567383 0.2084188312292099 -0.02787429839372635 0.5765863060951233 0.10159894824028015 -0.64273601770401 0.33416715264320374 0.3804006576538086 -0.1551746279001236 -0.5385252237319946 0.20193244516849518 -0.25455185770988464 -0.15579907596111298 0.46022093296051025 0.022593725472688675 0.2250465452671051 -0.014071380719542503 0.22293786704540253 0.035297732800245285 -0.1746470034122467 0.20926572382450104 0.10677561908960342 0.5286217331886292 0.14160414040088654 -0.29738637804985046 -0.24915964901447296 -0.14470887184143066 0.32141438126564026 -0.40098220109939575 0.1498265415430069 0.31993308663368225 -0.17123685777187347 -0.008353144861757755 -0.5630977749824524 -0.24290475249290466 -0.2937528192996979 -0.4244062602519989 0.22268052399158478 -0.2697306275367737 -0.12315235286951065";
//		
//		
//		
//		str2 = str2.substring(0, str2.length() - 1);
//
//		System.out.println("Tail cut-off: " + str2);
//
//		String curr = "";
//
//		int i1 = 0;
//
//		while (str2.length() > 10) {
//			i1 = str2.indexOf('|');
//			if (i1 >= 0) {
//				curr = str2.substring(0, i1);
//				if (str1.compareTo(curr) > 0)
//					break;
//				if (i1 + 1 < str2.length()) {
//					str2 = str2.substring(i1 + 1, str2.length());
//				}
//
//			} else {
//				break;
//			}
//		}
//
//		System.out.println("Most relevant timestamp: " + curr);
//
//		System.out.println("Entity: "
//				+ test.substring(test.lastIndexOf('/') + 1, test.length()));
//
//		System.out.println("themamichael jackson".indexOf("michael_jackson"
//				.replace('_', ' ')));
//
//		System.out.println(AnchorTimeSpan(time_series));
//
//		System.out.println("Matched: "
//				+ StringUtils.countMatches("Angela",
//						"Angela Angela Merkel Angela Merkel"));
//
//		System.out.println(getReverse("www.klinikum.uni-heidelberg.de"));
//
//		merge_anchor();
//
//		String[] arr = "String ".split(" ", 2);
//
//		System.out.println(getDest_url("de,angela-merkel)/Angela_merkel"));
//
//		System.out.println(arr[1]);
//
//		System.out.println("Matched: "
//				+ ("45,12,3".matches("^[-+]?\\d+([\\.\\,]\\d+)*$")));
//
//		int i = 0;
//		try {
			// for (i = 0; i < MAX_LENGTH; i++) {
			// test += "a";
			// }
//			System.out.println("size: " + test.length());
//
//			System.out.println("URL: "
//					+ URLDecoder.decode("de,angela-merkel)/Angela%2C+merkel",
//							"UTF-8"));
//		} catch (Exception e) {
//			System.out.println(i);
//		}
		// String s =
		// "(brown) \"fox\" 'c';foo bar || 55.555;\"foo\";'bar')(;{ gray fox=-56565.4546; foo boo=\"hello\"{";
		// Pattern p =
		// Pattern.compile("([\"'])(?:[^\"']+|(?!\\1)[\"'])*\\1|\\|\\||<=|&&|[()\\[\\]{};=#]|[\\w.-]+");
		// Pattern p =
		// Pattern.compile("(?:([\"']?)[-]?[a-z0-9-.]*\1|(?<=[^a-z0-9])[^a-z0-9](?=(?:[^a-z0-9]|$))|(?<=[a-z0-9\"'])[^a-z0-9\"'](?=(?:[^a-z0-9]|['\"]|$)))",
		// Pattern.CASE_INSENSITIVE);
		// Matcher m = p.matcher(s) ;

		// while (m.find()) {
		// System.out.println("item = `" + m.group() + "`");
		// }

		// System.out.println(QueryFunc("de,dw)/", "Jamie", "Jamie_Dornan"));

		// System.out.println(exec("com,lake26)/",
		// "{(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Homepage.),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;lake),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images),(com,lake26)/,&nbsp;Lake 26 - words, tales &amp; images)}"));
		// System.out.println(exec1("com,lake26)/abcde/fsdhfkjsdhgsd/jsdhgsdij"));
		//
//		String str = "de,channel21)/ponchos/cb_40289eea30adb0860130ae30884a31f5_40289eea30adb0860130ae342a455c5b/de?@queryterm=*&@sort.productinventory=1&@sort.productsortingorder=0&categoryuuidlevelx=mxtaqbqqxxcaaaezw45959r_&categoryuuidlevelx/mxtaqbqqxxcaaaezw45959r_=xdvaqbqq_faaaaezz55959r_&categoryuuidlevelx/mxtaqbqqxxcaaaezw45959r_/xdvaqbqq_faaaaezz55959r_=jabaqbqqatyaaaezv55959r_&categoryuuidlevelx/mxtaqbqqxxcaaaezw45959r_/xdvaqbqq_faaaaezz55959r_/jabaqbqqatyaaaezv55959r_=k4baqbqqataaaaezv55959r_&searchparameter=&sizecode=(46-52)";
		// String str1 =
		// "de,channel21)/ponchos/cb_40289eea30adb0860130ae30884a31f5_40289eea30adb0860130ae342a455c5b/de?@queryterm=*&@sort.productinventory=1&@sort.productsortingorder=0&categoryuuidlevelx=mxtaqbqqxxcaaaezw45959r_&categoryuuidlevelx/mxtaqbqqxxcaaaezw45959r_=xdvaqbqq_faaaaezz55959r_&categoryuuidlevelx/mxtaqbqqxxcaaaezw45959r_/xdvaqbqq_faaaaezz55959r_=jabaqbqqatyaaaezv55959r_&categoryuuidlevelx/mxtaqbqqxxcaaaezw45959r_/xdvaqbqq_faaaaezz55959r_/jabaqbqqatyaaaezv55959r_=k4baqbqqataaaaezv55959r_&searchparameter=&sizecode=(46-52)	(46-52)&nbsp;(3)	(46-52)&nbsp;(3)	(46-52)&nbsp;(3)	(46-52)&nbsp;(3)	(46-52)&nbsp;(3)";
		// int i = str.indexOf(")");
		// int j = str.indexOf(",");
		// String[] fields = str.split("[\t ]");
		// if ((j < i - 2) && (j > 0)) {
		// //System.out.println(processAnchorsTuple(fields[1], fields[0]));
		// System.out.println(SplitURL(str1));
		// //context.write(word, null);
		// }
		// System.out.println("" + i + "\t" + j);
		//
		// System.out.println(checkTime("2013-08-11T23:09:26.000Z", "day",
		// "11"));

//		GermanStemmer stemm = new GermanStemmer();
//		StopwordsGerman stopw = new StopwordsGerman();
//
//		System.out
//				.println("Stemmer: "
//						+ stemm.stemString("Wer im Mio Mio essen gehen möchte, steht zurzeit vor verschlossenen Türen. Das Metallgitter ist vorgezogen - aber eine Tafel verkündet, dass das beliebte Restaurant an der Deisterstraße ab September unter neuer Regie „in gewohnter Manier“ an den Start gehen wird. Das ist nicht die einzige Neuerung in der Lindener Gastronomie. Auch in den Räumen der seit einiger Zeit geschlossenen Kultkneipe Härtekrug in der Falkenstraße tut sich etwas. Die Inhaber der benachbarten Trattoria da Giorgio übernehmen das Lokal, um dort ihre italienische Küche anzubieten. Und für die Räume, die sie frei machen, gibt es auch schon einen Interessenten: Der Betreiber des afrikanischen Restaurants Kilimanjaro in der Altstadt will in Linden eine Zweigstelle eröffnen.")
//						+ ". Stopword: " + stopw.isStopword("gehen"));
//
//		System.out.println(computeFeature56(str));
//		System.out.println(computeFeature58(str));
//		System.out.println(computeFeature59(extractDomain(str)));
//		System.out.println(computeFeature60(str));
//		System.out.println(computeFeature61(str));
//		computeFeature57(extractDomain(str));
//		
//		
//		
//		System.out.println(vector.split("[\t ]", 2)[0]);
//		System.out.println(vector.split("[\t ]", 2)[1]);
//		
//		
//		String row = "558313278	de,webnews)/tag/chopin	chopin chopin 	2__Klavierkonzert		2.0	2	78.0	0.5	1	78	0	1	2	0	0	1.0	0.0	0	-22	0	2	0	12	1		74710533";
//		
//		String[] line = row.split("\t");
//		row = "";
//		for (i = 1; i < line.length; i ++) {
//			if (line[i].length() > 0)
//				row += (line[i] + "\t");
//		}
//		
//		if (row.lastIndexOf('\t') == row.length() - 1)  {
//			row = row.substring(0, row.length() -1);
//		}
//		
//		System.out.println("Row: " + row);
	}

	private static String SplitURL(String str) {
		int i = str.indexOf("\t");
		String URL = exec1(str.substring(0, i));
		String rest = str.substring(i + 1, str.length());
		rest = rest.replaceAll("[\t]", " | ");
		return URL + "\t" + StringEscapeUtils.unescapeHtml(rest);
	}

	private static String processAnchorsTuple(String str, String addr) {
		int len;
		if ((len = str.length()) > 4)
			str = str.substring(1, len - 1);
		else
			return null;

		String pattern = Pattern.quote("(" + addr + ",");
		String[] values = str.split(pattern, 2);
		len = values.length;
		str = "";
		int i = 0, k = 0;
		for (i = 0; i < len; i++) {
			if (values[i].length() == 0) {
				k++;
				continue;
			}

			// if (addr.length() > 0)
			// values[i] = values[i].substring(addr + 1);
			if (i < len - 1)
				values[i] = values[i].substring(0, values[i].length() - 2);
			else
				values[i] = values[i].substring(0, values[i].length() - 1);

			str += (values[i] + ((i == values.length - 1) ? "" : " | "));
		}
		return str + "\t" + (i - k);
	}

	public static String exec1(String input) {

		String str = input;
		int i = str.indexOf(")");
		int j = str.indexOf(",");
		if ((j > i - 1) || (j < 0))
			return null;
		String rest = str.substring(i + 1, str.length());
		str = str.replace(")", "");
		str = str.substring(0, i);
		String[] values = str.split(",");
		str = "";
		for (i = values.length - 1; i > 0; i--)
			str = str + values[i] + ".";
		str = str + values[0] + rest;
		values = null;
		rest = null;
		return str;
	}

	public static boolean checkTime(String strTime, String strPart, String value) {
		try {
			if (strTime == null)
				return false;
			if (strPart.toLowerCase().equals("year")) {
				if (strTime.substring(0, 4).equals(value))
					return true;
			}

			if (strPart.toLowerCase().equals("month")) {
				if (strTime.substring(5, 7).equals(value))
					return true;
			}

			if (strPart.toLowerCase().equals("day")) {
				if (strTime.substring(8, 10).equals(value))
					return true;
			}

			return false;
		} catch (Exception e) {
			System.out.println("Error");
		}
		return false;
	}

	public static String exec(String addr1, String input) {
		String str = input;
		String addr = addr1;
		addr = new StringBuilder(addr).insert(addr.indexOf(")"), "\\")
				.toString();
		str = str.substring(2, str.length() - 2);
		String[] values = str.split("\\),\\(");
		str = "";
		int i = 0, j = 0, k = 0;
		// reporter.progress();
		for (i = 0; i < values.length; i++) {
			if (values[i] == null) {
				k++;
				continue;
			}
			System.out.println(addr);
			values[i] = values[i].substring(addr.length());
			// reporter.progress();
			values[i] = org.apache.commons.lang.StringEscapeUtils
					.unescapeHtml(values[i]);
			str += (values[i] + " | ");
		}

		return str + "\t" + (i - k);
	}

	public static boolean QueryFunc(String dest_url, String anchors,
			String query_str) {
		StringTokenizer token = new StringTokenizer(query_str, "_");
		String currentToken = null;
		boolean found = true;
		while (found && token.hasMoreTokens()) {
			currentToken = token.nextToken();
			found = found
					&& ((dest_url.indexOf(currentToken) >= 0) || (anchors
							.indexOf(currentToken) >= 0));
		}
		return found;
	}

	public static String ExtractDomain(String str) {

		String value = null;
		try {
			if (str == null)
				return null;
			if (str.length() < 4)
				return null;
			int idx = str.indexOf(")");

			if (idx > 4)
				value = str.substring(0, idx);
			else
				value = null;

			return value;
		} catch (Exception e) {
			// throw new IOException("Caught exception processing input row " +
			// str, e);
		}
		return null;
	}

	static String extractDomain(String sURL) {
		String[] parts = sURL.split("\\)/");
		return parts[0];
	}

	static int computeFeature54(String fullAddress) {
		/*
		 * Count number of digit in domain of URL
		 */

		int domainidx = fullAddress.lastIndexOf(','), result = 0;
		String sDomain = "";

		if (domainidx > 0) {
			sDomain = fullAddress.substring(0, domainidx);
		}

		for (int j = 0, len = sDomain.length(); j < len; j++) {
			if (Character.isDigit(sDomain.charAt(j))) {
				result++;
			}
		}

		return result;
	}

	static int computeFeature55(String fullAddress) {
		/*
		 * Count number of digit in host name of URL
		 */

		int domainidx = fullAddress.lastIndexOf(','), result = 0;
		String sHost = "";

		if (domainidx > 0) {
			sHost = fullAddress.substring(domainidx + 1, fullAddress.length());
		}

		for (int j = 0, len = sHost.length(); j < len; j++) {
			if (Character.isDigit(sHost.charAt(j))) {
				result++;
			}
		}

		return result;
	}

	public static Integer AnchorTimeSpan(String content) {
		long duration = 0;
		DateTime d = null;
		Period p = null;

		DateTime BEGIN_DATE = DateTime.parse("1990-01-01");
		String[] series = content.split(" ");
		PriorityQueue<Long> spanSeries = new PriorityQueue<Long>();
		for (int i = 0; i < series.length; i++) {
			d = DateTime.parse(series[i].trim());
			duration = Days.daysBetween(BEGIN_DATE, d).getDays();
			spanSeries.add(duration);
		}
		// progress();

		int timeSpanCount = 0;
		duration = spanSeries.peek();
		long span = 0;

		while (!spanSeries.isEmpty()) {
			span = spanSeries.poll();
			if (Math.abs(span - duration) > 7) {
				timeSpanCount++;
			}
			duration = span;
		}

		return new Integer(timeSpanCount);
	}

	public static String getDest_url(String dest_url) {
		String decodeURL = "";

		try {
			decodeURL = URLDecoder.decode(dest_url, "UTF-8");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error URL: - " + dest_url);
			e.printStackTrace();
		}

		String first = "", later = "", revfirst = "";
		int split_pos = decodeURL.indexOf(')');
		if (split_pos >= 4) {
			first = decodeURL.substring(0, split_pos);
			String[] dn = first.split(",");
			int len = dn.length;
			revfirst = dn[0];
			for (int i = 1; i < len; i++) {
				revfirst = dn[i] + "." + revfirst;
			}
			later = decodeURL.substring(split_pos + 1);
		}
		return (revfirst + later).toLowerCase();
	}

	static int computeFeature56(String sURLAndPathAndQueryString) {
		/*
		 * Count number of parts in domain, path, and query string
		 */

		String sDomain = extractDomain(sURLAndPathAndQueryString);

		int countOfPartInDomain = StringUtils.countMatches(sDomain, ",");
		int countOfPartInPath = StringUtils.countMatches(
				sURLAndPathAndQueryString, "/");
		int countOfPartInQueryString = StringUtils.countMatches(
				sURLAndPathAndQueryString, "&");

		return countOfPartInDomain + countOfPartInPath
				+ countOfPartInQueryString;
	}

	static int computeFeature58(String sURLAndPathAndQueryString) {
		/*
		 * Count number of parts in query string
		 */

		int countOfPartInQueryString = StringUtils.countMatches(
				sURLAndPathAndQueryString, "&");

		return countOfPartInQueryString;
	}

	static int computeFeature59(String hostAndDomain) {
		/*
		 * Compute host length by character
		 */

		int domainidx = hostAndDomain.lastIndexOf(','), result = 0;
		String sHost = "";

		if (domainidx > 0) {
			sHost = hostAndDomain.substring(domainidx + 1,
					hostAndDomain.length());
		}

		return sHost.length();
	}

	static int computeFeature60(String sURLAndPathAndQueryString) {
		/*
		 * Compute path length by character
		 */

		String sPath = sURLAndPathAndQueryString.substring(
				sURLAndPathAndQueryString.indexOf('/') + 1,
				sURLAndPathAndQueryString.lastIndexOf('/'));

		return sPath.length();
	}

	static int computeFeature61(String sURLAndPathAndQueryString) {
		/*
		 * Compute query string length by character
		 */

		String sQuery = sURLAndPathAndQueryString.substring(
				sURLAndPathAndQueryString.indexOf('?') + 1,
				sURLAndPathAndQueryString.length());

		return sQuery.length();
	}

	static void computeFeature57(String hostAndDomain) {
		int domainidx = hostAndDomain.lastIndexOf(',');
		String sHost = "", sDomain = "";
		if ((domainidx > 0) && (domainidx < hostAndDomain.length() - 1)) {
			sDomain = hostAndDomain.substring(0, domainidx);
			sHost = hostAndDomain.substring(domainidx + 1,
					hostAndDomain.length());
		}
		
		System.out.println(sDomain + " " + sHost);
	}
}
