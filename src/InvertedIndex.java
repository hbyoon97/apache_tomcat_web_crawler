/* --
COMP4321 Lab1 Exercise
Student Name:
Student ID:
Section:
Email:
*/

import org.rocksdb.RocksDB;

import java.util.Set;
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

    public void addEntry(String word, int x, int y) throws RocksDBException
    {
        // Add a "docX Y" entry for the key "word" into hashtable
        // ADD YOUR CODES HERE
        byte[] content = db.get(word.getBytes());
        if(content != null){
            //append
            content = (new String(content) + " doc" + x + " " + y).getBytes();
        } else {
            //create new key value pair
            content = ("doc" + x + " " + y).getBytes();
        }   
        db.put(word.getBytes(), content);
    }

    public void delEntry(String word) throws RocksDBException
    {
        // Delete the word and its list from the hashtable
        // ADD YOUR CODES HERE
        db.remove(word.getBytes());
    } 

    public void printAll() throws RocksDBException
    {
        // Print all the data in the hashtable
        // ADD YOUR CODES HERE
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
        }
    }    
    
    // Get the docID by the URL (e.g. https://www.cse.ust.hk/ returns "doc0")
    public String getDocID(String url) throws RocksDBException {
    	String new_url = "docMapping_" + url;
    	byte[] content = db.get(new_url.getBytes());
    	return new String(content);
    }
    
    // Get the wordID by the word 
    public String getWordID(String word) throws RocksDBException {
    	String new_word = "wordMapping_" + word;
    	byte[] content = db.get(new_word.getBytes());
    	return new String(content);
    }
    
    // Get last-modified
//    public String getLastModified(String url) throws RocksDBException{
//    	String str = "metadata_" + getDocID(url);
//    	byte[] content = db.get(str.getBytes());
//    	String contentStr = content.toString();
//    	System.out.println("contentStr" + contentStr);
//    	String[] parts = contentStr.split(":");
//    	return parts[0];
//    }
    
    public void addPCRelation(String p_url, String c_url) throws RocksDBException{
    	String parentID = "PCR_" + getDocID(p_url);
    	String childID = getDocID(c_url);
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
    
    public void clear() throws RocksDBException {
    	RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            db.remove(new String(iter.key()).getBytes());
        }
    }
    
    public void forward(String url, String word, int count) throws RocksDBException{
    	String str = getDocID(url);
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
    
    public void invert(String url, String word, int freq) throws RocksDBException {
    	String docID = getDocID(url);
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
    
   public void metadata(String url, String lm, int size, String lang, int level) throws RocksDBException{
	   String str = "metadata_" + getDocID(url);
	   byte[] content = db.get(str.getBytes());
	   if(content != null) {
   		//append
   		content = (new String(content) + " " + lm + ":" + size + ":" + lang).getBytes();
   	} else {
   		//create new inverted_wordID -> docID freq
   		content = (lm + ":" + size + ":" + lang).getBytes();
   	}
   	db.put(str.getBytes(), content);
   }
   
    public static void main(String[] args)
    {
        try
        {
            // a static method that loads the RocksDB C++ library.
            RocksDB.loadLibrary();
            
            // modify the path to your database
            String path = "/Users/anthonykwok/Documents/Academic/HKUST/Year 2020-2021 (DSCT Yr 3)/2021 Spring Semester Course/COMP4321/Project/comp4321-project/db";

            InvertedIndex index = new InvertedIndex(path);
    
//            index.addEntry("cat", 2, 6);
//            index.addEntry("dog", 1, 33);
//            System.out.println("First print");
//            index.printAll();
//            
//            index.addEntry("cat", 8, 3);
//            index.addEntry("dog", 6, 73);
//            index.addEntry("dog", 8, 83);
//            index.addEntry("dog", 10, 5);
//            index.addEntry("cat", 11, 106);
//            System.out.println("Second print");
//            index.printAll();
//            
//            index.delEntry("dog");
//            System.out.println("Third print");
//            index.printAll();
            index.clear();
        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}
