/* --
COMP4321 Lab1 Exercise
Student Name:
Student ID:
Section:
Email:
*/

import org.rocksdb.RocksDB;

import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;  
import org.rocksdb.RocksIterator;

public class InvertedIndex
{
    private RocksDB db;
    private Options options;
    private int wordNextID;
    private int docNextID;
    
    // 
    InvertedIndex(String dbPath) throws RocksDBException
    {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.wordNextID = 0;
        this.docNextID = 0;

        // creat and open the database
        this.db = RocksDB.open(options, dbPath);
    }
    
    public void addDocMappingEntry(String url) throws RocksDBException{
    	byte[] content = db.get(url.getBytes());
    	
    	if(content != null){
            return;
        } else {
	        //create new key value pair
	        content = ("doc" + this.docNextID).getBytes();
	        this.docNextID++;
        }
        db.put(url.getBytes(), content);
    }
    
    public void addWordMappingEntry(String word) throws RocksDBException{
    	byte[] content = db.get(word.getBytes());
  
    	if(content != null){
            return;
        } else {
            //create new key value pair
            content = ("word" + this.wordNextID).getBytes();
            this.wordNextID++;
        }   
        db.put(word.getBytes(), content);
    }

    public void delEntry(String word) throws RocksDBException
    {
        // Delete the word and its list from the hashtable
        // ADD YOUR CODES HERE
        db.remove(word.getBytes());
    }  
    
    // Get the docID by the URL (e.g. https://www.cse.ust.hk/ returns "doc0")
    public String getDocIDbyURL(String url) throws RocksDBException {
    	String result = "";
    	String new_url = "docMapping_" + url;
    	byte[] content = db.get(new_url.getBytes());
    	if(content != null) {
    		result = new String(content);
    	}
    	return result;
    }
    
    public String getURLbyDocID(String docID) throws RocksDBException{
    	RocksIterator iter = db.newIterator();
    	String url = "";
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		String value = new String(iter.value());
    		if(value.equals(docID)) {
    			url = new String(iter.key());
    			url = url.replace("docMapping_","");
    			break;
    		}
    	}
    	return url;
    }
    
    // Get the wordID by the word 
    public String getWordID(String word) throws RocksDBException {
    	String new_word = "wordMapping_" + word;
    	byte[] content = db.get(new_word.getBytes());
    	return new String(content);
    }
    
    // Get last-modified
    public String getLastModified(String url) throws RocksDBException{
    	String str = "metadata_" + getDocIDbyURL(url);
    	byte[] content = db.get(str.getBytes());
    	String contentStr = content.toString();
    	System.out.println("contentStr" + contentStr);
    	String[] parts = contentStr.split(":");
    	return parts[0];
    }
    
    public Vector<String> getURLList()throws RocksDBException {
    	Vector<String> urlsList = new Vector<String>();
    	RocksIterator iter = db.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		String parent = new String(iter.key());
    		
    		// Check only Parent-Child Relationship
    		if(parent.contains("PCR_")) {
    			parent = parent.replace("PCR_","");
    			// Get back the url by the docID
    			parent = getURLbyDocID(parent);
    			// Add parent to URL List if no exist
    			if(!urlsList.contains(parent)) {
    				urlsList.add(parent);
    			}
//    			// Add child to URL List if no exist
//    			String value = new String(iter.value());
//    			String[] children = value.split(" ");
//    			for (String child:children) {
//    				if(!urls.contains(child)) {
//        				urls.add(child);
//        			}
//    			}
    		}   	
    	}
    	return urlsList;
    }
    
    public void addPCRelation(String p_url, String c_url) throws RocksDBException{
    	String parentID = "PCR_" + getDocIDbyURL(p_url);
    	String childID = getDocIDbyURL(c_url);
    	byte[] content = db.get(parentID.getBytes());
    	if(content != null){
            //append
    		String old_child = new String(content);
    		if (!old_child.contains(childID)) {
    			content = (new String(content) + " " + childID).getBytes();
    		}
            
        } else {
            //create new key value pair
            content = (childID).getBytes();
        }   
        db.put(parentID.getBytes(), content);
    }
    
    public String getPCRelation(String p_url) throws RocksDBException{
    	String result = new String();
    	String parentID = "PCR_" + getDocIDbyURL(p_url);
    	byte[] content = db.get(parentID.getBytes());
    	if(content != null){
    		String new_content = new String(content);
    		String[] children = new_content.split(" ");
    		int count = 0;
    		for (String child: children){
    			count++;
    			result = result + "Child Link " + String.valueOf(count) + ": " + getURLbyDocID(child) + "\n";
    		}
    		return result;
    	}else {
    		return "";
    	}

    }
    
    public void forward(String url, String word, int count) throws RocksDBException{
    	String str = getDocIDbyURL(url);
    	str = "forward_" + str;
    	byte[] content = db.get(str.getBytes());
    	if(content != null){
            //append
            content = (new String(content) + " " + word + ":" +String.valueOf(count)).getBytes();
        } else {
            //create new key value pair
            content = (word + ":" + String.valueOf(count)).getBytes();
        }   
        db.put(str.getBytes(), content);
    }
    
    public String getForward(String url)throws RocksDBException{
    	String str = getDocIDbyURL(url);
    	str = "forward_" + str;
    	byte[] content = db.get(str.getBytes());
    	String result = "";
    	if(content != null){
            //append
    		String new_content = new String(content);
    		String[] keywordPair = new_content.split(" ");
    		for(String pair: keywordPair) {
    			String keyword = pair.split(":")[0];
    			String freq = pair.split(":")[1];
    			result = result + keyword + " " + freq + "; ";
    		}
        }
        return result + "\n";
    }
    
    public void invert(String url, String word, int freq) throws RocksDBException {
    	String docID = getDocIDbyURL(url);
    	String wordID = getWordID(word);
    	String str = "inverted_" + wordID;
    	
    	byte[] content = db.get(str.getBytes());
    	if(content != null) {
    		//append
    		content = (new String(content) + " " + docID + ":" + freq).getBytes();
    	} else {
    		//create new inverted_wordID -> docID freq
    		content = (docID + ":" + freq).getBytes();
    	}
    	db.put(str.getBytes(), content);
    }
    
   public void metadata(String url, String title, String lm, int size, String lang, int level) throws RocksDBException{
	   String str = "metadata_" + getDocIDbyURL(url);
	   byte[] content = db.get(str.getBytes());
	   if(content != null) {
   		//append
   		content = (new String(content) + " " + title + ":" + lm + ":" + size + ":" + lang).getBytes();
   	} else {
   		//create new inverted_wordID -> docID freq
   		content = (title + ":" + lm + ":" + size + ":" + lang).getBytes();
   	}
   	db.put(str.getBytes(), content);
   }
   
   public Vector<String> getMetadata(String url) throws RocksDBException{
	   Vector<String> result = new Vector<String>();
	   String str = "metadata_" + getDocIDbyURL(url);
	   byte[] content = db.get(str.getBytes());
	   if(content != null) {
		   String data = new String(content);
		   String[] metadata =data.split(":");
		   for (String d:metadata) {
			   result.add(d);
		   }
	   }
	   
	   return result;
   }

   public void clear() throws RocksDBException {
   	RocksIterator iter = db.newIterator();
       for(iter.seekToFirst(); iter.isValid(); iter.next()){
           db.remove(new String(iter.key()).getBytes());
       }
   }
   
   public void printAll() throws RocksDBException {
   	// Print all the data in the hashtable
       // ADD YOUR CODES HERE
       RocksIterator iter = db.newIterator();
       for(iter.seekToFirst(); iter.isValid(); iter.next()){
           System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
       }
   }   
   
    public static void main(String[] args)
    {
        try
        {
            // a static method that loads the RocksDB C++ library.
            RocksDB.loadLibrary();
            
            // modify the path to your database
            String path = "/db";

            InvertedIndex index = new InvertedIndex(path);
            index.clear();
        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}
