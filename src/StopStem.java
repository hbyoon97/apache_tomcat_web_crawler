import java.util.*;
import java.io.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class StopStem {
    private Porter porter;
    private java.util.HashSet<String> stopWords;
    private Vector<String> source;
    private Vector<String> stemmedWords = new Vector<String>();

    public boolean isStopWord(String str) {
        return stopWords.contains(str);
    }

    public StopStem(String stopwordSrc) {
        super();
        porter = new Porter();
        stopWords = new java.util.HashSet<String>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(stopwordSrc));
            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Stem the word
    public String stem(String str) {
        return porter.stripAffixes(str);
    }

    // Import the source for word stemming
    public void importSource(Vector<String> source) {
        this.source = source;
    }
    
    // Print out the source for word stemming
    public void printSource() {
        System.out.println("\nSource Words: " + this.source + "\n");
    }
    
    // Print out the stemmed word
    public void printStemmedWord() {
    	Collections.sort(this.stemmedWords);
        System.out.println("\nStemmed Words: " + this.stemmedWords + "\n");
    }

    // Stem the word
    public Vector<String> StemWord() {
        for (String word : this.source) {
            if (this.isStopWord(word)) {
//                System.out.println("It should be stopped");
            } else {
                String stemmed = this.stem(word);
//                System.out.println("The stem of it is \"" + stemmed + "\"");
                if (stemmed != "") {
                    this.stemmedWords.add(stemmed);
                }
            }
            continue;
        }
        return this.stemmedWords;
    }
    
    
    // Main Program in Stop Stem
    public static void main(String[] arg) {

        StopStem stopStem = new StopStem("stopwords-en.txt");

        String[] testString = { "Modification", "Hello", " Stop", "?", "番緊?", "#$!@$@!" };
        List<String> testList = Arrays.asList(testString);
        Vector<String> testing = new Vector<String>(testList);

        stopStem.importSource(testing);
        stopStem.printSource();
        stopStem.StemWord();
        stopStem.printStemmedWord();

    }
}
