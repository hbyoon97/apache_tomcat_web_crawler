# COMP4321 Project - Web Search Engine

## Team Information

KWOK, Ue Nam			SID:20597580	(unkwok@connect.ust.hk)
YOON, Han Byul			SID:20385808	(hbyoon@connect.ust.hk)
TSUI, Yuen Yau			SID:20607488	(yytsuiab@connect.ust.hk)

## External Library Usage

1. jsoup-1.13.1.jar
2. rocksdbjni-6.18.0-linux64.jar

## Instructions to build the spider program:

Note: Instructions are designed for COMP4321 CentOS VM environment, or any other UNIX-based OS such as Linux and Mac OS X (Please use the correspondingly appropriate jar files for compilation)

0.Clone this repo. 

git clone git@github.com:yhb1010/comp4321-project.git

1.Open terminal and change directory to comp4321-project

cd comp4321-project

2.Run "java -version" and "javac -version" to confirm java is installed

java -version
javac -version

3.Run the following command one by one to compile the program:

javac -d bin -sourcepath src ./src/Porter.java
javac -d bin -sourcepath src ./src/StopStem.java
javac -d bin -sourcepath src -cp ./lib/rocksdbjni-6.18.0-linux64.jar ./src/InvertedIndex.java
javac -d bin -sourcepath src -cp ./lib/rocksdbjni-6.18.0-linux64.jar:./lib/jsoup-1.13.1.jar ./src/Crawler.java

4.Run the following command to execute the Crawler:

java -cp bin:./lib/rocksdbjni-6.18.0-linux64.jar:./lib/jsoup-1.13.1.jar Crawler

5. For reference, the console will output relevant information from the crawling, including the indexing of entire key value mappings in db

## Result
A "spider_result.txt" will be created and stored in the current directory 


