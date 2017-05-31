import util.SimpleUtil;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.payload_XML.generatedclasses.Employee;


public class Testing 
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
	
	public static void main(String args[])
	{
		Employee employee = new Employee();
		employee.setFirstName("ddd");
		employee.setLastName("ddd");
		System.out.println(SimpleUtil.convert_object_to_JSON(employee));

	}

}
