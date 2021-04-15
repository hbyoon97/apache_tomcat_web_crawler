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
import java.util.Map.Entry;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
	 * @throws RocksDBException 
	 */
	public Response getResponse(Link focus, InvertedIndex index) throws HttpStatusException, IOException {
		if (this.urls.contains(focus.url)) {
			throw new RevisitException(); // if the page has been visited, break the function
		 }
		
		// Connection conn = Jsoup.connect(url).followRedirects(false);
		// the default body size is 2Mb, to attain unlimited page, use the following.
		Connection conn = Jsoup.connect(focus.url).maxBodySize(0).followRedirects(false).ignoreHttpErrors(true);
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
				 this.urls.add(focus.url);
			 }
		} catch (HttpStatusException e) {
			throw e;
		}
		
		/* Get the metadata from the result */
		String lastModified = res.header("last-modified");
		int size = res.bodyAsBytes().length;
		String title = res.parse().title();
		String htmlLang = res.parse().select("html").first().attr("lang");
		String bodyLang = res.parse().select("body").first().attr("lang");
		String lang = htmlLang + bodyLang;

		try {
			index.metadata(focus.url, title, lastModified, size, lang, focus.level);
		} catch (RocksDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
        	// filter out emails and empty and valueless hrefs
        	if (linkString.contains("mailto:") || linkString.contains("#") || linkString.contains("javascript:") 
        			|| linkString.equals("")) {
        		continue;
        	} 
            result.add(link.attr("href"));
        }
        return result;
	}
	
	/** Print all info about the focused page to console
	 */
	public void printPageInfo(Response res, Document doc, Link focus) {
		System.out.println("Page title: " + doc.title());
		System.out.println("URL: " + focus.url);
		System.out.printf("Last Modified: %s\t", res.header("last-modified"));
		System.out.printf("Page Size: %d Bytes\n", res.bodyAsBytes().length);
	}
	
	/** Print page's words and links
	 */
	public void printWordsAndLinks(Link focus, Set<Entry<String, Integer>> keywordFreqPair, Vector<String> links) {	
		System.out.println("\nWords:");
		 for (Entry<String, Integer> entry : keywordFreqPair) {
	        	System.out.print(entry.getKey() + " " + entry.getValue() + "; ");
	        }
		System.out.printf("\n\nLinks:\n");
		
		int index = 1;
		for(String link: links) {
			System.out.println(String.format("Child Link %d: %s", index++, link));
			if(this.urls.contains(link)) {
				continue;
			}
			this.URLqueue.add(new Link(link, focus.level + 1)); // add links
		}
		System.out.println("-------------------------------------------------------------------------------------------");
	}
	
	/** Use a URLqueue to manage crawl tasks.
	 */
	public void crawlLoop(InvertedIndex index) {
		int count = 0;
		
		// To map the first link (i.e. https://www.cse.ust.hk/)
		Link source_link = URLqueue.get(0);
		Vector<String> head_link = new Vector<String>();
		head_link.add(source_link.url);
		docMapping(head_link, index);
		
		while(!this.URLqueue.isEmpty()) {
			Link focus = this.URLqueue.remove(0);
			/* start to crawl on the page */
			try {
				if (count++ == 30) break; // stop criteria
				Response res = this.getResponse(focus, index);
				Document doc = res.parse();
				
				if(res.parse().title().isEmpty() || res.statusCode() == 301 || res.statusCode() == 302) {
					continue;
				}
				
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
				StopStem stopStem = new StopStem("lib/stopwords-en.txt");
				stopStem.importSource(words);
		        words = stopStem.StemWord();
		        Collections.sort(words);
		        
		        Set<Entry<String, Integer>> keywordFreqPair = forwardIndex(focus.url, words, index);
					
		        //Print spider_result fashion output to console
				printPageInfo(res, doc, focus);
				printWordsAndLinks(focus, keywordFreqPair, links);
				
				// Creating URL and docID Mapping
				docMapping(links, index);
				
				// Creating Word and WordID Mapping
				wordMapping(words, index);
				
				// Creating Inverted File
				invertedIndexing(focus.url, keywordFreqPair, index);
				
				// Creating Parent-Child Relationship
				parentChild(focus.url, links, index);
				
			} catch (Exception e){ 
				count--;
				continue;
			}
		}
		
	}
	
	public void invertedIndexing(String url, Set<Entry<String, Integer>> keywordFreqPair, InvertedIndex index) {
        for (Entry<String, Integer> entry : keywordFreqPair) {
			try {
				index.invert(url, entry.getKey(), entry.getValue());
			} catch (RocksDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	}
	
	public void docMapping(Vector<String> links,InvertedIndex index){
		for(String url: links) {
			try {
				String newUrl = "docMapping_" + url;
				index.addDocMappingEntry(newUrl);
			} catch (RocksDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void wordMapping(Vector<String> words, InvertedIndex index) {
		for(String word: words) {
			try {
				String newWord = "wordMapping_" + word;
				index.addWordMappingEntry(newWord);
			} catch (RocksDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void parentChild(String p_url,Vector<String> children, InvertedIndex index) {
		for(String child_url: children) {
			try {
				index.addPCRelation(p_url,child_url);
			} catch (RocksDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/** initialize Set with Entry<keyword, frequency>
	 */
	public Set<Entry<String, Integer>> forwardIndex(String url, Vector<String> words, InvertedIndex index) {
		//forward_docID -> (word, freq)
		 Map<String, Integer> wordAndCount = new HashMap<String, Integer>();

		 for (String word : words) {
			 Integer count = wordAndCount.get(word);

            if (count == null) {
                wordAndCount.put(word, 1);
            } else {
                wordAndCount.put(word, ++count);
            }
        }

        Set<Entry<String, Integer>> entrySet = wordAndCount.entrySet();
        for (Entry<String, Integer> entry : entrySet) {
        	try {
				index.forward(url, entry.getKey(), entry.getValue());
			} catch (RocksDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        return entrySet;
	}
	
	/** rocksdb connection helper
	 */
	public static InvertedIndex RocksDBConnection() {
		 InvertedIndex index = null;
		 try{
             // a static method that loads the RocksDB C++ library
             RocksDB.loadLibrary();

             // modify the path to your database
             String path = "db";

             index = new InvertedIndex(path);
             index.clear();
         }
         catch(RocksDBException e)
         {
             System.err.println(e.toString());
         }
		 return index;
	}
	
	public String getInfo(Crawler crawler,InvertedIndex index) {

		String result = "";
		try {
			HashSet<String> urls = crawler.urls;
			for(String url: urls) {
				Vector<String> metadata = index.getMetadata(url);
				
				// get the page title
				if(metadata.size()!=0)
					result = result + "Page Title: " + metadata.get(0) + "\n";
		
				// get the url
				result = result + "URL: " + url + "\n";
		
				// get last modification date
				if(metadata.size()!=0)
					result = result + "Last Modification Date: " + metadata.get(1);
		
				// get page size
				if(metadata.size()!=0)
					result = result + ", Size of page: " + metadata.get(2) + "\n";
				
				// get forward indexing keyword freq
				result = result + index.getForward(url);
				
				// get the child link
				result = result + index.getPCRelation(url);
				
				result = result + "------------------------------------------------------------------\n";
			}
		}catch(RocksDBException e) 
		{
			System.err.println(e.toString());
		}
		return result;
	}

	public void outputTXT(String content){
		try {
		    BufferedWriter writer = new BufferedWriter(new FileWriter("./spider_result.txt"));
		    writer.write(content);
		    writer.close();
		    System.out.println("Output spider_result.txt successfully");
		}
		catch(IOException e)
		{
			 System.err.println(e.toString());
		}
}
	
	public static void main (String[] args) {
		InvertedIndex index = RocksDBConnection();
		String url = "https://www.cse.ust.hk/";
		Crawler crawler = new Crawler(url);
		crawler.crawlLoop(index);
		try {
			index.printAll();
		} catch (RocksDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String final_output = crawler.getInfo(crawler,index);
		crawler.outputTXT(final_output);
		System.out.println("\nSuccessfully Returned");
	}
		
}

