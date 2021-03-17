/**
COMP4321 Lab2 Exercise
Student name:
Student ID:
ITSC:
*/

import java.util.*;
import org.jsoup.Jsoup;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import org.jsoup.HttpStatusException;
import java.lang.RuntimeException;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;  
import org.rocksdb.RocksIterator;

/** The data structure for the crawling URLqueue.
 */
class Link{
	String url;
	int level;
	Link (String url, int level) {  
	    this.url = url;
	    this.level = level; 
	}  
}

@SuppressWarnings("serial")
/** This is customized exception for those pages that have been visited before.
 */
class RevisitException 
	extends RuntimeException {
	public RevisitException() {
	    super();
	}
}

public class Crawler {
	private HashSet<String> urls;     // the set of urls that have been visited before
	public Vector<Link> URLqueue; // the queue of URLs to be crawled
//	private int max_crawl_depth = 1;  // feel free to change the depth limit of the spider.
	
	Crawler(String _url) {
		this.URLqueue = new Vector<Link>();
		this.URLqueue.add(new Link(_url, 1));
		this.urls = new HashSet<String>();
	}

	/**
	 * Send an HTTP request and analyze the response.
	 * @return {Response} res
	 * @throws HttpStatusException for non-existing pages
	 * @throws IOException
	 */
	public Response getResponse(String url) throws HttpStatusException, IOException {
		if (this.urls.contains(url)) {
			throw new RevisitException(); // if the page has been visited, break the function
		 }
		
		// Connection conn = Jsoup.connect(url).followRedirects(false);
		// the default body size is 2Mb, to attain unlimited page, use the following.
		Connection conn = Jsoup.connect(url).maxBodySize(0).followRedirects(false).ignoreHttpErrors(true);
		Response res;
		
		try {
			/* establish the connection and retrieve the response */
			 res = conn.execute();
			 /* if the link redirects to other place... */
			 if(res.hasHeader("location")) {
				 String actual_url = res.header("location");
				 if (this.urls.contains(actual_url)) {
					throw new RevisitException();
				 }
				 else {
					 this.urls.add(actual_url);
				 }
			 }
			 else {
				 this.urls.add(url);
			 }
		} catch (HttpStatusException e) {
			throw e;
		}
		/* Get the metadata from the result */
		// String lastModified = res.header("last-modified");
		 int size = res.bodyAsBytes().length;
		 String htmlLang = res.parse().select("html").first().attr("lang");
		 String bodyLang = res.parse().select("body").first().attr("lang");
		 String lang = htmlLang + bodyLang;
		// System.out.printf("Last Modified: %s\n", lastModified);
		// System.out.printf("Size: %d Bytes\n", size);
		// System.out.printf("Language: %s\n", lang);
		return res;
	}

	/** Extract words in the web page content.
	 * note: use StringTokenizer to tokenize the result
	 * @param {Document} doc
	 * @return {Vector<String>} a list of words in the web page body
	 */
	public Vector<String> extractWords(Document doc) {
		 Vector<String> result = new Vector<String>();
		// ADD YOUR CODES HERE
		 String contents = doc.body().text(); 
	     StringTokenizer st = new StringTokenizer(contents);
	     while (st.hasMoreTokens()) {
            result.add(st.nextToken());
	     }
	     return result;		
	}
	
	/** Extract useful external urls on the web page.
	 * note: filter out images, emails, etc.
	 * @param {Document} doc
	 * @return {Vector<String>} a list of external links on the web page
	 */
	public Vector<String> extractLinks(Document doc) {
		Vector<String> result = new Vector<String>();
		// ADD YOUR CODES HERE
        Elements links = doc.select("a[href]");
        for (Element link: links) {
        	String linkString = link.attr("href");
        	// filter out emails and empty/valueless hrefs
        	if (linkString.contains("mailto:") || linkString.contains("#") || linkString.contains("javascript:") 
        			|| (linkString.length() > 0 && linkString.charAt(0) == '/') || linkString.equals("")) {
        		continue;
        	} 
            result.add(link.attr("href"));
        }
        return result;
	}
	
	/** Print all info about the focused page
	 */
	public void printPageInfo(Response res, Document doc, Link focus, int count) {		
		System.out.println(count);
		System.out.println("Page title: " + doc.title());
		System.out.println("URL: " + focus.url);
		System.out.printf("Last Modified: %s\t", res.header("last-modified"));
		System.out.printf("Page Size: %d Bytes\n", res.bodyAsBytes().length);
	}
	
	/** Print all info about the focused page
	 */
	public void printWordsAndLinks(Link focus, Vector<String> words, Vector<String> links) {	
		System.out.println("\nWords:");
		System.out.println(words);
		System.out.printf("\n\nLinks:\n");
		
		int index = 1;
		for(String link: links) {
			System.out.println(String.format("Child Link %d: %s", index++, link));
			if(this.urls.contains(link)) continue;
			this.URLqueue.add(new Link(link, focus.level + 1)); // add links
		}

		System.out.println("-------------------------------------------------------------------------------------------");
		
	}
	
	/** Use a URLqueue to manage crawl tasks.
	 */
	public void crawlLoop() {
		int count = 0;

		while(!this.URLqueue.isEmpty()) {
			Link focus = this.URLqueue.remove(0);
			if (count++ == 1) break; // stop criteria
			if (this.urls.contains(focus.url)) continue;   // ignore pages that has been visited
			/* start to crawl on the page */
			try {
				Response res = this.getResponse(focus.url);
				Document doc = res.parse();

				Vector<String> words = this.extractWords(doc);
				Vector<String> links = this.extractLinks(doc);
				
				//handle Chinese characters
				Iterator<String> iter = words.iterator();
				while(iter.hasNext()){
					String str = iter.next();
					if(str.matches("[\\u4E00-\\u9FA5]+")) 
						iter.remove();
				} 

				//stemming before passing
//				System.out.println(new File(".").getAbsoluteFile());
				StopStem stopStem = new StopStem("lib/stopwords-en.txt");
				stopStem.importSource(words);
		        words = stopStem.StemWord();
		        stopStem.printStemmedWord();
				
				printPageInfo(res, doc, focus, count);
				printWordsAndLinks(focus, words, links);
				

			} catch (Exception e){ 
				System.out.println(e);
				System.out.println(focus.url);
			}
		}
		
	}
	
	public static void main (String[] args) {
		 try{
             // a static method that loads the RocksDB C++ library
             RocksDB.loadLibrary();

             // modify the path to your database
             String path = "/home/tommyyoon/eclipse-workspace/comp4321-project/db";

             InvertedIndex index = new InvertedIndex(path);
             index.printAll();
         }
         catch(RocksDBException e)
         {
             System.err.println(e.toString());
         }
		 
		String url = "https://www.cse.ust.hk/";
		Crawler crawler = new Crawler(url);
		crawler.crawlLoop();
		System.out.println("\nSuccessfully Returned");
	}
}

	