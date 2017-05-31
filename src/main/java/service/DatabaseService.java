package service;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.json.JSONException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class DatabaseService 
{
	static MongoClient mongoClient; 
	static DB mongoDB;
	static DBCollection mongoDataCollection;
	public static DB getMongoDB() 
	{
		if (mongoDB == null) {
			mongoDB = getMongoClient().getDB("db1");
		}
		return mongoDB;
	}
	public static DBCollection getDBCollection() 
	{
		if (mongoDataCollection == null) {
			mongoDataCollection = getMongoDB().getCollection("coll1");
		}
		return mongoDataCollection;
	}
	
	public static MongoClient getMongoClient() {
		if (mongoClient == null) {
			mongoClient = new MongoClient("localhost", 27017);
		}
		return mongoClient;
	}
	
	public static int flush_logs(String inp_json) 
	{
		DBObject dbObject = (DBObject) JSON.parse(inp_json);
		int insertcount =0;
		try
		{
			WriteResult writeResult = getDBCollection().insert(dbObject);
		}
		catch(java.lang.UnsupportedOperationException e)
		{
			e.printStackTrace();
			insertcount =-1;
		}
		
		return insertcount;
	}
	
	public static List<org.json.JSONObject> query_db_object(String jsonString)
	{
		System.out.println("Search:"+ jsonString);
		if(jsonString == null && jsonString.length()==0)
		{
			jsonString = new String("{}");
		}
		BasicDBObject obj = BasicDBObject.parse(jsonString);
		System.out.println(obj.toJson());
		DBCursor cur = getDBCollection().find(obj);
		System.out.println("done serach");
		System.out.println(cur.count());
		List <org.json.JSONObject> outputList = new ArrayList();
		while(cur.hasNext())
		{
			DBObject result= cur.next();

			
			org.json.JSONObject output;
			try 
			{
				output = new org.json.JSONObject(JSON.serialize(result));
				outputList.add(output);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(outputList);
		return outputList;
	}
	
	public static int delete_db_object(String jsonString)
	{
		System.out.println("Delete:"+ jsonString);
		int deleteCount =0;
		if(jsonString == null && jsonString.length()==0)
		{
			jsonString = new String("{}");
		}
		BasicDBObject obj = BasicDBObject.parse(jsonString);
		System.out.println(obj.toJson());
		try
		{
			WriteResult writeResult = getDBCollection().remove(obj);
			deleteCount = writeResult.getN();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			deleteCount =-1;
		}
		
		return deleteCount;
	}
}
