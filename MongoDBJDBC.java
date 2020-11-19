 
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.math.*;
import java.io.*;
import java.awt.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.*; 
import java.lang.Object;
import java.util.regex.*;
import java.util.Scanner;
import java.util.*;
import java.text.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Arrays;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Filters;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.Block;
import org.bson.BsonDocument;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import java.net.UnknownHostException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import org.bson.Document;
public class MongoDBJDBC{
 public static void main( String args[] ){
  try{   

   MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
   MongoDatabase mongoDatabase = mongoClient.getDatabase("final");  
   Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
   mongoLogger.setLevel(Level.OFF);   
   Scanner scan = new Scanner(System.in);
   System.out.println();
   System.out.println("Welcome to my analyze system!");
   System.out.println();
   //Creating Menu
   while(true){
    System.out.println("********************************************");
    System.out.println("1.Check the number of country he mentioned");
    System.out.println("2.Check the tweets data by month");
    System.out.println("3.Check the country he mentioned most");
    System.out.println("4.Check the number of tweets he posted in given datetime");
    System.out.println("5.MapReduceFunction");
    System.out.println("9.Exit system");
    System.out.println("********************************************");
    System.out.println();
    System.out.println("Enter your choice::");
    int choice = scan.nextInt();

    switch(choice){
      case 1:  
      	CheckByCountry(mongoDatabase);
      	break;
      case 2:  
      	CheckByMonth(mongoDatabase);
      	break;
      case 3:  
      	TheMostCount(mongoDatabase);
      	break;
      case 4: 
      	GroupBytime(mongoDatabase,scan);
      	break;
      case 5: 
        MapReduce(mongoDatabase,scan);
      	break;
      case 9: System.out.println("Thanks for using system");

      System.exit(0);
      	break;
      	default: System.out.println("Incorrect input!");
    }
  }
}
	catch(Exception e){
	  System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	}
}

public static void CheckByCountry (MongoDatabase mongoDatabase)  {
  List<String> clist=new ArrayList<String>();
  MongoCollection<Document> collect = mongoDatabase.getCollection("2016_12_05");
  MongoCollection<Document> country = mongoDatabase.getCollection("country-abbreviation");
 
  for (Document c : country.find()){
    clist.add(c.getString("country"));
  }

  Map<String, Integer>  hm =  new HashMap();

  for (Document doc : collect.find()){
    String s=doc.getString("Tweet");
    for(String curC : clist ){  
      boolean flag = s.contains(curC);
      if(flag){
        if(!hm.containsKey(curC)){
          hm.put(curC,1);
        }
        else{
          Integer ss=hm.get(curC)+1;
          hm.put(curC, ss++);
        }
      } 
    }
  }
  System.out.println("Trump mentioned ["+hm.size()+"] cities in his Tweet");
  System.out.println();
}



public static void GroupBytime (MongoDatabase mongoDatabase,Scanner scan)  {
  Block<Document> printBlock = new Block<Document>() {
    @Override
    public void apply(final Document document) {
      System.out.println(document.toString());
    }
  };
  String instart;
  String inend;

  System.out.println("Welcome");
  instart = scan.nextLine();

  System.out.println("Enter [startDate] you want to Check: Type :0 to check all");
  instart = scan.nextLine();


  System.out.println("Enter [endDate] you want to check: Type :0 to check all");
  inend = scan.nextLine();
 
  MongoCollection<Document> collect = mongoDatabase.getCollection("2016_12_05");
 
  try {
   SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    if(instart.equals("0")){

      instart="1990-01-01";
    }
   
    if(inend.equals("0")){

      inend="2020-01-01";
    }
    Date sTime =  format.parse(instart+" 00:00:00");
    Date eTime =  format.parse(inend+" 00:00:00");
  
    collect.aggregate(
    Arrays.asList( 
      Aggregates.match(Filters.regex("Tweet", "^.{10,}$")),
      Aggregates.match(Filters.gt("Date", sTime)),
      Aggregates.match(Filters.lt("Date", eTime)),
      Aggregates.group("$Date", Accumulators.sum("count", 1)),
      Aggregates.match(Filters.gt("count", 0)),
      new Document("$sort", new Document("count", -1)),
      Aggregates.project(Projections.include("Date","count")))
    ).forEach(printBlock);
  }
  catch (Exception e) {
    System.out.println(e);
  }
}

public static void TheMostCount (MongoDatabase mongoDatabase)  {
  MongoCollection<Document> collect = mongoDatabase.getCollection("2016_12_05");
  MongoCollection<Document> country = mongoDatabase.getCollection("country-abbreviation");
  List<String> clist=new ArrayList<String>();
  for (Document c : country.find()){
    clist.add(c.getString("country"));
  }
  Map<String, Integer>  hm =  new HashMap();

  for (Document doc : collect.find()){
    String s=doc.getString("Tweet");
    for(String curC : clist ){
      boolean flag = s.contains(curC);
      if(flag){
        if(!hm.containsKey(curC)){
          hm.put(curC,1);
        }
        else{
          Integer ss=hm.get(curC)+1;
          hm.put(curC, ss++);
        }
      } 
    }
  }
  List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(hm.entrySet());
  Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
    public int compare(Map.Entry<String, Integer> mapping1, Map.Entry<String, Integer> mapping2) {
      return mapping2.getValue().compareTo(mapping1.getValue());
    }
  });
  for (Map.Entry<String, Integer> mapping : list) {
    System.out.println(mapping.getKey() + " ：" + mapping.getValue());
  }
}

public static void CheckByMonth (MongoDatabase mongoDatabase){
  String date;
  Scanner scan = new Scanner(System.in);
  System.out.println("Welcome");
  System.out.println("Enter [date] you want to add: Type : 2015-03-01[yyyy-mm-dd]");
  date = scan.nextLine();

  MongoCollection<Document> collect = mongoDatabase.getCollection("2016_12_05");
  SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
  try{
    Date sTime =  format.parse(date+" 00:00:00");
    List<Document> dateList=new ArrayList<Document>();
    Integer idx=1;
    for (Document doc : collect.find(eq("Date", sTime))){
      String conte= doc.getString("Tweet");
      System.out.println(idx+"]"+conte);
      System.out.println("");
      dateList.add(doc);
      idx++;
    }
  System.out.println("He posted ["+dateList.size()+"] Tweets");
  System.out.println();
  }
  catch (Exception e) {
    System.out.println(e);
  }
}

public static void MapReduce (MongoDatabase mongoDatabase,Scanner scan)  {

  MongoClient mongoClient = new MongoClient("localhost", 27017);
 
  DB db = mongoClient.getDB("final");
  DBCollection ofertas = db.getCollection("2016_12_05");
  
  String c;
  String wordcnt;
  String map;

  System.out.println("Welcome");
  c = scan.nextLine();

  System.out.println("Enter [country] you want to Check: Type :0 to check all");
  c = scan.nextLine();

  System.out.println("Enter [wordcnt] you want to check: Type :0 to check all");
  wordcnt = scan.nextLine();



  try{
    String bothquery = "function () { var reg = RegExp(/"+c+"/); if (this.Tweet.length>="+wordcnt+"&&reg.test(this.Tweet)){emit(this.Client, 1);} }";
    String mCounty="function () { var reg = RegExp(/"+c+"/); if (reg.test(this.Tweet)){emit(this.Client, 1);} }";
    String mWord="function () { var reg = RegExp(/"+c+"/); if (this.Tweet.length>="+wordcnt+"){emit(this.Client, 1);} }";
    String curc="function () {  emit(this.Client, 1);}";
    String reduce = " function(key, values) {   return Array.sum(values) }";
    String com="0";
    if(c.equals(com) && !wordcnt.equals(com)){
      map=mWord;
    }
    if(!c.equals(com)&& wordcnt.equals(com)){
      map=mCounty;
    }
    if(!c.equals(com) && !wordcnt.equals(com)){
      map =bothquery;
    }
    if(c.equals(com) && wordcnt.equals(com)){
      map=curc;
    }

    MapReduceCommand cmd = new MapReduceCommand(ofertas, map, reduce, null, MapReduceCommand.OutputType.INLINE, null);
    MapReduceOutput out = ofertas.mapReduce(cmd);

    for (DBObject o : out.results()) {
      System.out.println(o);
    }
  }
    catch (Exception e) {
     System.out.println(e);
    }
  }
}
