package service;

import util.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.simple.JSONObject;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;
import com.sun.jersey.spi.resource.Singleton;

/**
 * @author reeta
 * This is the class which invoked when REST call is made
 * This class implements all t  he methods of logging
 */
@Path("/logging")
@Singleton
public class LoggingService 
{
	static Cache cache;
	static Region<String, String> transactionRegion;
	static Region<String, Log> logRegion;
	static Region<String, String> applicationRegion;
	static final String DATABASE_LOG = "Database";
	static final String EVENT_LOG = "Event";
	static final String POLICY_LOG = "Policy";
	static final String DEVICE_LOG = "Device";
	static Connection conn = null;	
	
	
	static
	{
		System.out.println("Initializing databases");
		try
		{
		getCache();
		getTransactionRegion();
		getApplicationRegion();
		getLogRegion();
		
		DatabaseService.getDBCollection();
		Thread.sleep(10000);
		
		//CONNECT TO MYSQL
		
		Class.forName("com.mysql.jdbc.Driver");
			
		conn = DriverManager.getConnection("jdbc:mysql://localhost/LOGGING?" + 
				"user=root&password=reeta");
		cleanup_service();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("Error in initializing database");
			
		}
	}
	public LoggingService()
	{
		super();	
	}


	
	public static Cache getCache()
	{
		if (cache == null) {
			cache = new CacheFactory().setPdxPersistent(true).setPdxSerializer(new ReflectionBasedAutoSerializer("Log"))
					.create();
		}
		return cache;
	}

	public static Region<String, String> getTransactionRegion() {
		if (transactionRegion == null) {
			transactionRegion = getCache()
					.<String, String> createRegionFactory()
					.setValueConstraint(String.class)
					.setKeyConstraint(String.class).create("transaction");
		}
		return transactionRegion;
	}

	public static Region<String, String> getApplicationRegion()
	{
		if (applicationRegion == null) {
			applicationRegion = getCache()
					.<String, String> createRegionFactory()
					.setValueConstraint(String.class)
					.setKeyConstraint(String.class).create("application");
		}
		return applicationRegion;
	}

	public static Region<String, Log> getLogRegion() {
		if (logRegion == null) {
			logRegion = getCache()
					.<String, Log> createRegionFactory()
					.setValueConstraint(Log.class)
					.setKeyConstraint(String.class).create("log");
		}
		return logRegion;
	}


	/**
	 * @return SuperResultObject
	 * Method to start a transactional log
	 */
	@GET
	@Path("/begin")
	@Produces(MediaType.APPLICATION_JSON)
	public SuperResultObject getTid() 
	{
		getLogRegion();
		String tid = java.util.UUID.randomUUID().toString();
		ResultObject resultObject = new ResultObject();
		try {
			getTransactionRegion().put(tid.toString(), "");
			resultObject.message = "Begin Transaction Logging Successfull";
			resultObject.success = true;
		} catch (Exception e) {
			e.printStackTrace();
			tid = "";
			resultObject.message = e.getLocalizedMessage();
			resultObject.success = false;
		}
		// return Response.status(200).entity(output).build();
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("tid", tid);
		SuperResultObject superResultObject = new SuperResultObject();
		superResultObject.result = jsonObject;
		superResultObject.status = resultObject;
		return superResultObject;
	}


	/**
	 * @param inputdata
	 * @return superResultObject
	 * This method writes Log to the Apache Geode database
	 */
	@POST
	@Path("/write")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public SuperResultObject write_log(InputData inputdata)
	{
		ResultObject result = create_log(inputdata);

		JSONObject jsonObject = new JSONObject();
		ResultObject resultObject = new ResultObject();
		SuperResultObject superResultObject = new SuperResultObject();

		if(result.success == true)
		{
			jsonObject.put("lsn", result.message);
			resultObject.message = "Log written successfully";
			resultObject.success = true;
		}
		else
		{
			jsonObject.put("lsn", "");
			resultObject.message = result.message;
			resultObject.success = false;
		}
		superResultObject.result = jsonObject;
		superResultObject.status = resultObject;

		return superResultObject;
	}


	/**
	 * @param inputDataList
	 * @return List of SuperResultObject
	 */
	@POST
	@Path("/write_multiple")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public List<SuperResultObject> write_multiple_logs(List<InputData> inputDataList)
	{
		List<SuperResultObject> superResultObjectList = new ArrayList();
		for(InputData inputData: inputDataList)
		{
			superResultObjectList.add(write_log(inputData));
		}
		return superResultObjectList;
	}



	/**
	 * @param inputdata
	 * @return
	 * This method writes log on the apache geode
	 */
	private ResultObject create_log(InputData inputdata) 
	{
		ResultObject result = new ResultObject();
		result.success = false;

		Boolean isTransaction = false;
		String prevLsnId = "";
		String nextLsnId = null;

		if (inputdata != null) 
		{
			if (inputdata.payload == null || inputdata.payload.length() ==0) {
				result.message = "Payload absent";
				return result;
			} 
			else if (inputdata.logtype == null || inputdata.logtype.length() ==0)
			{
				result.message = "Logtype absent";
				return result;
			}
			else if(!(inputdata.logtype.equalsIgnoreCase(DATABASE_LOG) || inputdata.logtype.equalsIgnoreCase(EVENT_LOG) || inputdata.logtype.equalsIgnoreCase(POLICY_LOG) || inputdata.logtype.equalsIgnoreCase(DEVICE_LOG)))
			{
				result.message = "logtype should be either of Database, Device, Policy, Event";
				return result;
			}
			//if transactional log
			if (inputdata.tid != null)
			{
				isTransaction = true;
				try 
				{
					if (getTransactionRegion().get(inputdata.tid.toString()) == null) {
						result.message = "Invalid tid";
						return result;
					}
					else
					{
						prevLsnId = getTransactionRegion().get(inputdata.tid.toString());
					}
				} catch (Exception e) {
					e.printStackTrace();
					result.message = "Encountered error in writing Log - " + e.getLocalizedMessage();
					return result;
				}
			}
			//if non transactional log
			else
			{	if(inputdata.appid ==null)
			{
				result.message = "A non-transactional log should have appid";
				return result;
			}
			try 
			{
				if (getApplicationRegion().get(inputdata.appid.toString()) == null) {
					getApplicationRegion().put(inputdata.appid.toString(), "");
				}
				else
				{
					prevLsnId = getApplicationRegion().get(inputdata.appid.toString());
				}
			} catch (Exception e) {
				e.printStackTrace();
				result.message = "Encountered error in writing Log - " + e.getLocalizedMessage();
				return result;
			}
			}

			UUID lsn = java.util.UUID.randomUUID();
			Log log = null;
			String payLoadString = inputdata.payload;
			Object payLoadObject = SimpleUtil.XML_to_Object(payLoadString);

			//Validate payLoad object
			if(payLoadObject instanceof Integer)
			{
				Integer integer = (Integer)payLoadObject;

				//it is not a XML
				if(integer ==100)
				{
					result.message = "Either a JSON or plane string";
				}
				else if(integer == 200)
				{
					result.message = "No XSD document found for given xml";
					return result;
				}
				else if(integer == 300)
				{
					result.message = "XML document validation failed";
					return result;
				}
			}

			if (isTransaction == false) 
			{
				log = new Log("", lsn.toString(), "", prevLsnId, new Date(),
						inputdata.logtype, inputdata.payload, inputdata.appid);
			}
			else 
			{
				log = new Log(inputdata.tid.toString(), lsn.toString(), "", prevLsnId,
						new Date(), inputdata.logtype, inputdata.payload, "");
			}

			try 
			{
				//insert new log
				getLogRegion().put(lsn.toString(), log);

				//update the most recent lsn for a transaction
				if(isTransaction == true)
				{
					getTransactionRegion().put(inputdata.tid.toString(), lsn.toString());
				}
				//update the most recent lsn for non-transaction (having appid)
				else
				{
					getApplicationRegion().put(inputdata.appid.toString(), lsn.toString());
				}
				// update the next lsn for prev Log
				if(prevLsnId !=null && prevLsnId.length() > 0)
				{
					Log prevLog = getLogRegion().get(prevLsnId);
					prevLog.next = lsn.toString();
					getLogRegion().put(prevLsnId, prevLog);
				}

			} catch (Exception e) {
				e.printStackTrace();
				result.message = "Encountered error in writing Log - " + e.getLocalizedMessage();
				result.success = false;
				return result;
			}

			result.message = lsn.toString();
			result.success = true;
		}
		else {
			result.message = "Input cannot be null or blank";
		}
		return result;
	}


	/**
	 * @param inputdata
	 * @return
	 * This method puts data from Apache geode to Mongo DB
	 */
	@PUT
	@Path("/flush")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public ResultObject flush_log(InputData inputData)
	{
		ResultObject result = new ResultObject();

		if(inputData!=null && inputData.lsn != null)
		{
			String queryString = "select v from /log.values v where 1=1";
			queryString = queryString + " and v.lsn = " +  "'" + inputData.lsn + "'";
			System.out.println(queryString);
			List <Log> logs = new ArrayList();
			Boolean cont = true;
			do	
			{
				QueryService queryService = getCache().getQueryService();
				Query query = queryService.newQuery(queryString);
				List <Log> tempLogs = new ArrayList();
				SelectResults results = null;

				try
				{
					results = (SelectResults)query.execute();
					tempLogs = results.asList();
					if(tempLogs.size() > 0)
					{
						Log logObject = tempLogs.get(0);
						logs.add(logObject);
						queryString =  "select v from /log.values v where v.lsn = " + "'" + logObject.prev +"'";
						System.out.println(queryString);
					}	
					else
					{
						cont = false;
					}
				} 
				catch (FunctionDomainException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TypeMismatchException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NameResolutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (QueryInvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			while(cont == true);
			System.out.println("Records to flush" + logs.size());
			int successfullInsert = 0;
			result.success = true;
			for(int i=logs.size()-1;i>=0;i--)
			{
				String payLoadJson =null;
				String dataInputToMongo = null;
				try 
				{
					Log logObject = logs.get(i);
					String logJson = SimpleUtil.convert_object_to_JSON(logObject);
					System.out.println("Log json" + logJson);

					
					Object payLoadObject = SimpleUtil.XML_to_Object(logObject.payload.toString());
					if(payLoadObject instanceof Integer)
					{
						Integer integer = (Integer)payLoadObject;
						if(integer == 100 )
						{
							dataInputToMongo = logJson;
						}
					}
					else
					{
						payLoadJson= SimpleUtil.convert_object_to_JSON(payLoadObject);
						System.out.println("Payloadjson" + payLoadJson);
						dataInputToMongo = logJson.replace("}", "") + payLoadJson.replace("{", ",");
					}
					
					System.out.println("Mongo input" + dataInputToMongo);
					int insertCount = DatabaseService.flush_logs(dataInputToMongo);
					System.out.println(insertCount);
					if(insertCount >=0)
					{
						successfullInsert = successfullInsert + insertCount;
					}
					else
					{
						successfullInsert=-1;
						break;
					}

				} 
				catch (IllegalArgumentException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
					result.success = false;
					result.message = e.getLocalizedMessage();
					return result;
				}	
			}
			if(successfullInsert  >= 0)
			{
				result.success = true;
				result.message = "Successfully flushed " + logs.size() + " entries";
			}
		}
		else
		{
			result.message = "Provide lsn";
		}
		return result;	
	}

	/**
	 * @param inputData
	 * @return
	 * This method is to commit a transaction. it flushes transactional logs from apache geode to mongodb
	 */
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("/commit")
	public ResultObject commit_logs(InputData inputData) 
	{
		ResultObject resultObject = new ResultObject();
		resultObject.success = false;
		String queryString = "select v from /log.values v where 1=1";

		if (inputData != null && inputData.tid !=null) 
		{
			try 
			{
				if (getTransactionRegion().get(inputData.tid.toString()) == null)
				{
					resultObject.message =  "Invalid Transaction id";
					return resultObject;
				}
			}
			catch(Exception e)
			{
				return resultObject;
			}

			queryString = queryString + " and v.tid = " + "'" + inputData.tid +"'" ;
			QueryService queryService = getCache().getQueryService();
			System.out.println(queryString);
			Query query = queryService.newQuery(queryString);

			List <Log> logs = new ArrayList();
			SelectResults results;

			try
			{
				results = (SelectResults)query.execute();
				logs = results.asList(); 

				if(logs.size() ==0)
				{
					resultObject.message ="No entry found with with matching criteria on tid";
				}

				else
				{
					int successfullInsert = 0;
					String sql = "INSERT INTO UNDO_LOG (tid, lsn, flushed)" + "VALUES (?, ?, ?)";
					for(int i=0;i<logs.size();i++)
					{
						System.out.println("Logging tid on mysql");
						java.sql.PreparedStatement preparedStatement = conn.prepareStatement(sql);
						
						preparedStatement.setString(1, logs.get(i).tid.toString());
						preparedStatement.setString(2, logs.get(i).lsn.toString());
						preparedStatement.setBoolean(3, false);
						
						preparedStatement.executeUpdate(); 
					}
					
					for(int i=0;i<logs.size();i++)
					{
						String payLoadJson =null;
						String dataInputToMongo = null;
						Log logObject = logs.get(i);
						String logJson = SimpleUtil.convert_object_to_JSON(logObject);
						System.out.println("Log json" + logJson);
						
						Object payLoadObject = SimpleUtil.XML_to_Object(logObject.payload.toString());
						if(payLoadObject instanceof Integer)
						{
							Integer integer = (Integer)payLoadObject;
							if(integer == 100 )
							{
								dataInputToMongo = logJson;
							}
						}
						else
						{
							payLoadJson= SimpleUtil.convert_object_to_JSON(payLoadObject);
							System.out.println("Payloadjson" + payLoadJson);
							dataInputToMongo = logJson.replace("}", "") + payLoadJson.replace("{", ",");
						}
						
						System.out.println("Mongo input" + dataInputToMongo);
						
						int insertCount = DatabaseService.flush_logs(dataInputToMongo);
						System.out.println(insertCount);
						if(insertCount >=0)
						{
							successfullInsert = successfullInsert + insertCount;
						}
						else
						{
							successfullInsert=-1;
							break;
						}
					}
					if(successfullInsert < 0)
					{
						resultObject.success = true;
						resultObject.message = "Encountered error in flushing entries";
					}
					else
					{
						resultObject.success = true;
						resultObject.message = "Successfully flushed " + successfullInsert + " entries";
						update_tran_status(inputData.tid.toString());
					}
					
				}
			}
			catch (Exception e)
			{
				
				e.printStackTrace();
				resultObject.message ="Erorr enountered in flush -" + e.getLocalizedMessage();
				make_transaction_atomic(inputData.tid.toString());
			}	
		}
		else
		{
			resultObject.message = "Provide tid to commit";
		}
		return resultObject;
	}
	
	
	/**
	 * @param tid
	 * If flushed = false, it means transaction did not commit properly
	 * Remove all the partial entries belonging to this transaction from mongo db database
	 */
	private static void make_transaction_atomic(String tid)
	{
		System.out.println("Making transaction atomic");
		 String query = "SELECT * FROM UNDO_LOG WHERE tid = " + "'" + tid +"'" + "AND flushed = false";
		 try
		 {
			 // create the java statement
			 java.sql.Statement st = conn.createStatement();
	      
			 // execute the query, and get a java resultset
			 java.sql.ResultSet rs = st.executeQuery(query);
	      
			 // iterate through the java resultset
			 List <String> lsn_list = new ArrayList();
			 while (rs.next())
			 {
				 String lsn = rs.getString("lsn");
				 System.out.format("%s", lsn);
				 lsn_list.add(lsn);
			 }
			 st.close();
			 Boolean success = true;
			 for(String lsn:lsn_list)
			 {
				 try
				 {
					 String mong_delete_query = "{" + "lsn" + ":" + "'" + lsn + "'" + "}";
					 System.out.println("Deleting from mongo - " + mong_delete_query);
					 DatabaseService.delete_db_object(mong_delete_query);
				 }
				 catch(Exception e1)
				 {
					 success = false;
					 e1.printStackTrace();
				 }
			 }
			 update_tran_status(tid);
		 }
		 catch(Exception e)
		 {
			 e.printStackTrace();
		 }
	}
	
	/**
	 * @param tid
	 * This method updates the flushed attribute to inform that the transaction is successfully flushed to disk or compleetly aborted
	 */
	private static void update_tran_status(String tid)
	{
			System.out.println("Updating transaction flushed to true");
			String sql = "UPDATE UNDO_LOG SET flushed = true WHERE tid = " + "'" + tid +"'" ;
			
			
			try {
				java.sql.Statement preparedStatement = conn.createStatement();
				preparedStatement.execute(sql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	}
	
	@DELETE
	@Path("/rollback")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public ResultObject rollback_logs(InputData inputData) 
	{
		getLogRegion();
		ResultObject result = new ResultObject();
		result.success = false;

		if (inputData!=null && inputData.tid != null) 
		{
			try {
				if (getTransactionRegion().get(inputData.tid.toString()) == null) {
					result.message = "Invalid tid";
					return result;
				}
				String queryString = "select v.lsn from /log.values v where v.tid = " + "'" + inputData.tid +"'";
						
				System.out.println(queryString);

				QueryService queryService = getCache().getQueryService();

				Query query = queryService.newQuery(queryString);

				SelectResults results;
				List<String> lsns = new ArrayList();
				try {
					results = (SelectResults) query.execute();
					System.out.println(results.size());
					lsns = results.asList();
					if (lsns.size() == 0) {
						result.message = "No entry found with tid - "
								+ inputData.tid.toString();
						result.success = false;
						return result;
					} else {
						getLogRegion().removeAll(lsns);
						getTransactionRegion().remove(inputData.tid.toString());
						result.message = "Successfully rolledback";
					}
					result.success = true;

				} catch (Exception e) {
					e.printStackTrace();
					result.message = "Faced error in rollback -"
							+ e.getLocalizedMessage();
				}

			} catch (Exception e) {
				e.printStackTrace();
				result.message = "Faced error in rollback -"
						+ e.getLocalizedMessage();
			}
		} else {
			result.message = "Provide tid to rollback";
		}
		return result;
	}

	private List<Log> query_db(InputData inputdata) throws ClassNotFoundException 
	{
		List<Log> logs = new ArrayList();
		String output = "query logs";
		String result;

		Boolean hasPayload = false;
		Boolean hasLogtype = false;
		Boolean hasLSN = false;
		Boolean hasStartTime = false;
		Boolean hasEndTime = false;
		String queryString = "";
		List<String> queries = new ArrayList<String>();
		String finalString = new String();
		if (inputdata != null) 
		{	
			Boolean addComma = false;
			if (inputdata.tid != null && inputdata.tid.toString().length() > 0) {
				queryString = queryString + "tid' = " + "'"
						+ inputdata.tid.toString() + "'";
				addComma = true;
				queries.add("tid");
				queries.add(inputdata.tid.toString());
			}
			else if(inputdata.appid != null && inputdata.appid.toString().length() > 0) {
				queryString = queryString + "appid' = " + "'"
						+ inputdata.appid.toString() + "'";
				addComma = true;
				queries.add("appid");
				queries.add(inputdata.appid.toString());

			}

			if (inputdata.lsn != null && inputdata.lsn.toString().length() > 0) {
				hasLSN = true;
				if(addComma == true)
				{
					queryString = queryString + ",";
				}
				queryString = queryString + "'lsn' = " + "'"
						+ inputdata.lsn.toString() + "'";
				queries.add("lsn");
				queries.add(inputdata.lsn.toString()  );
			}
			if (inputdata.logtype != null
					&& inputdata.logtype.toString().length() > 0) {
				hasLogtype = true;
				if(addComma == true)
				{
					queryString = queryString + ",";
				}
				queryString = queryString + "'logtype' = " + "'"
						+ inputdata.logtype.toString() + "'";
				queries.add("logtype");
				queries.add(inputdata.logtype.toString() );
			}
			String newPayloadJsonString =null;
			String payloadJsonString = null;
			if(inputdata.payload !=null && inputdata.payload.length() > 0)
			{
				hasPayload = true;
				
				try
				{
					Object payLoadObject = SimpleUtil.XML_to_Object(inputdata.payload);
					if(payLoadObject instanceof Integer)
					{
						queries.add("payload");
						queries.add(inputdata.payload.toString());
						newPayloadJsonString = null;
					}
					else
					{
						payloadJsonString = SimpleUtil.convert_object_to_JSON(payLoadObject);
						System.out.println(payloadJsonString);
						newPayloadJsonString = payloadJsonString.substring(1,payloadJsonString.length()-1 );
					}

				} 
				catch (IllegalArgumentException  e)
				{
					e.printStackTrace();
				}
			}
			//TODO start time and end time

			String myJsonstring = "";
			Object object [] = queries.toArray();
			for (int i=0;i<object.length ;i+=2)
			{
				if(myJsonstring.length() > 0)
				{
					myJsonstring = myJsonstring + ",";
				}
				myJsonstring = myJsonstring + "\"" + (object[i].toString().replace("\"", "")) + "\"" + " : " + "\""+ object[i+1].toString().replace("\"", "") +"\"" ;

			}

			if(myJsonstring.length() > 0)
			{
				if(hasPayload == true && newPayloadJsonString !=null)
				{
					finalString = "{ " +  myJsonstring + " , " + newPayloadJsonString + " }";
				}
				else
				{
					finalString = "{ " +  myJsonstring + "}";
				}
			}
			else
			{
				if(hasPayload == true && newPayloadJsonString !=null)
				{
					finalString =  "{ " + newPayloadJsonString +" }";
				}

			}
		}
		List<org.json.JSONObject> jsonObjectList = DatabaseService.query_db_object(finalString);
		List<Log> logObjectList = new ArrayList();
		try
		{
			for(org.json.JSONObject jsonObject:jsonObjectList )
			{
				Log logObject = new Log();
				
				try
				{
					logObject.appid = jsonObject.getString("appid");
				}
				catch(Exception e)
				{
					logObject.appid="";	
				}
				try
				{
				logObject.lsn = jsonObject.getString("lsn");
				}
				catch(Exception e)
				{
					logObject.lsn="";
				}
				try
				{
				logObject.logtype = jsonObject.getString("logtype");
				}
				catch(Exception e)
				{
					logObject.logtype ="";	
				}
				try
				{
				logObject.payload = (String)jsonObject.get("payload");
				}
				catch(Exception e)
				{
					logObject.payload="";	
				}
				try
				{
				logObject.tid = jsonObject.getString("tid");
				}
				catch(Exception e)
				{
					logObject.tid ="";	
				}
				try
				{
				logObject.prev = jsonObject.getString("prev");
				}
				catch(Exception e)
				{
					logObject.prev="";	
				}
				try
				{
				logObject.next = jsonObject.getString("next");
				}
				catch(Exception e)
				{
					logObject.next="";
				}
				try
				{
				logObject.date = new Date(jsonObject.getString("date"));
				}
				catch(Exception e)
				{
				logObject.date= new Date();	
				}

				logObjectList.add(logObject);				
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return logObjectList;
	}
	
	
	
	/**
	 * @param inputdata
	 * @return List of logs that satisfy search criteria
	 * @throws ClassNotFoundException
	 * This method queries database (mongodb as well as main memory database)
	 */
	@POST
	@Path("/query")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Set<Log> query_main(InputData inputdata) throws ClassNotFoundException
	{
		Set<Log> result_logs = new HashSet();
		List<Log> logs = new ArrayList();
		List<Log> memory_logs = new ArrayList();
		if(inputdata != null)
		{
			if((inputdata.lsn !=null && inputdata.lsn.toString().length() >0) || (inputdata.tid!=null && inputdata.tid.toString().length() >0))
			{
				System.out.println("Querying main memory");
				memory_logs = query_log_memory(inputdata);
				if(memory_logs.size() ==0)
				{
					System.out.println("Querying DB memory");
					logs = query_db(inputdata);
					save_to_main_memory(logs);
				}
				else
				{
					logs = memory_logs;
				}
			}
			else
			{
				System.out.println("Querying DB memory");
				logs = query_db(inputdata);
				//There might be data which is not yet flushed to disk, we can try finding that in main memory
				memory_logs = query_log_memory(inputdata);
				
				if(logs.size() ==0)
				{
					logs = memory_logs;
				}
				else
				{
					//save to main memory for future reference
					save_to_main_memory(logs);
					logs.addAll(memory_logs);
				}
			}
		}
		Map<String, Log> map = new HashMap();
		for(Log log:logs)
		{
			if(!(map.containsKey(log.lsn)))
			{
				map.put(log.lsn, log);
			}
		}
		result_logs.addAll(map.values());
		return result_logs;
	}
	
	private void save_to_main_memory(List<Log> logs)
	{
		for(Log log: logs)
		{
			System.out.println("Saving data to main memory");
			getLogRegion().put(log.lsn.toString(), log);
		}
	}
	

	private static void cleanup_service() 
	{
		 System.out.println("Calling cleanup service");
		 String query = "SELECT distinct tid FROM UNDO_LOG WHERE flushed = false";
		 try
		 {
			 // create the java statement
			 java.sql.Statement st = conn.createStatement();
	      
			 // execute the query, and get a java resultset
			 java.sql.ResultSet rs = st.executeQuery(query);
	      
			 // iterate through the java resultset
			 List <String> tid_list = new ArrayList();
			 while (rs.next())
			 {
				 String tid = rs.getString("tid");
				 tid_list.add(tid);
			 }
			 System.out.println("To clean up - " + tid_list.size());
			 for (String tid:tid_list)
			 {
				 make_transaction_atomic(tid);
			 }
			 st.close();
		 }
		 catch(Exception e)
		 {
			 e.printStackTrace();
		 }
	}

	private List<Log> query_log_memory(InputData inputdata) {
		List<Log> logs = new ArrayList();
		String output = "query logs";
		String result;

		Boolean hasPayload = false;
		Boolean hasLogtype = false;
		Boolean hasLSN = false;
		Boolean hasStartTime = false;
		Boolean hasEndTime = false;

		if (inputdata != null) {
			String queryString = "select * from /log.values v where 1=1";

			if (inputdata.tid != null && inputdata.tid.toString().length() > 0) {
				queryString = queryString + " and v.tid = " + "'"
						+ inputdata.tid.toString() + "'";
			}
			else if(inputdata.appid != null && inputdata.appid.toString().length() > 0) {
				queryString = queryString + " and v.appid = " + "'"
						+ inputdata.appid.toString() + "'";

			}

			if (inputdata.lsn != null && inputdata.lsn.toString().length() > 0) {
				hasLSN = true;
				queryString = queryString + " and v.lsn = " + "'"
						+ inputdata.lsn.toString() + "'";
			}
			if (inputdata.logtype != null
					&& inputdata.logtype.toString().length() > 0) {
				hasLogtype = true;
				queryString = queryString + " and v.logtype = " + "'"
						+ inputdata.logtype.toString() + "'";
			}
			if (inputdata.payload != null && inputdata.payload.length() > 0) 
			{
				hasPayload = true;
				queryString = queryString + " and v.payload = " + "'"
						+ inputdata.payload.toString() + "'";
			}
			/*
			if ((inputdata.starttime != null && inputdata.starttime.toString()
					.length() > 0)
					&& (inputdata.endtime != null && inputdata.endtime
					.toString().length() > 0)) {
				hasStartTime = true;
				queryString = queryString + " and v.date between "
						+ inputdata.starttime.toString() + " and "
						+ inputdata.endtime.toString();
			}*/
			
			System.out.println(queryString);

			QueryService queryService = getCache().getQueryService();

			Query query = queryService.newQuery(queryString);

			SelectResults results;
			try {
				results = (SelectResults) query.execute();
				logs = results.asList();
			} catch (FunctionDomainException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TypeMismatchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NameResolutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (QueryInvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return logs;
		} else {
			return logs;
		}
	}
	
	@POST
	@Path("/delete")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public ResultObject delete(InputData inputdata)  
	{
		ResultObject resultObject = new ResultObject();
		resultObject.message = "Successfully deleted";
		resultObject.success = true;
		List<Log> logs = new ArrayList();
		List<Log> memory_logs = new ArrayList();
		try
		{
			if(inputdata != null)
			{
				logs = query_db(inputdata);
				delete_db(inputdata);
				
				delete_log(inputdata);
				for(Log log: logs)
				{
					if(getLogRegion().containsKey(log.lsn.toString()))
					{
						getLogRegion().remove(log.lsn.toString());
					}
				}
			}
		}
		catch(Exception e)
		{
			resultObject.message ="Encountered exception in delete log - " +e.getLocalizedMessage();
			resultObject.success=false;
		}
		return resultObject;
	}
	
	public ResultObject delete_log(InputData inputdata) {
		ResultObject result = new ResultObject();
		result.success = false;
		List<String> lsns = new ArrayList();

		Boolean hasPayload = false;
		Boolean hasLogtype = false;
		Boolean hasLSN = false;
		Boolean hasStartTime = false;
		Boolean hasEndTime = false;

		if (inputdata != null) {
			String queryString = "select v.lsn from /log.values v where 1=1";

			if (inputdata.tid != null && inputdata.tid.toString().length() > 0) {
				queryString = queryString + " and v.tid = " + "'"
						+ inputdata.tid.toString() + "'";
			}
			else if(inputdata.appid !=null && inputdata.appid.toString().length() >0)
			{
				queryString = queryString + " and v.appid = " + "'"
						+ inputdata.appid.toString() + "'";
			}

			if (inputdata.lsn != null && inputdata.lsn.toString().length() > 0) {
				hasLSN = true;
				queryString = queryString + " and v.lsn = " + "'"
						+ inputdata.lsn.toString() + "'";
			}
			if (inputdata.logtype != null
					&& inputdata.logtype.toString().length() > 0) {
				hasLogtype = true;
				queryString = queryString + " and v.logtype = " + "'"
						+ inputdata.logtype.toString() + "'";
			}
			if ((inputdata.starttime != null && inputdata.starttime.toString()
					.length() > 0)
					&& (inputdata.endtime != null && inputdata.endtime
					.toString().length() > 0)) {
				hasStartTime = true;
				queryString = queryString + " and v.date between "
						+ inputdata.starttime.toString() + " and "
						+ inputdata.endtime.toString();
			}
			if (inputdata.payload != null && inputdata.payload.length() > 0) {
				hasPayload = true;
				queryString = queryString + " and v.payload = " + "'"
						+ inputdata.payload.toString() + "'";
			}
			if (queryString.equals("select * from /log.values v where 1=1")) {
				result.message = "Give some criteria to delete log";
				return result;
			}

			System.out.println(queryString);

			QueryService queryService = getCache().getQueryService();

			Query query = queryService.newQuery(queryString);

			SelectResults results;
			try {
				results = (SelectResults) query.execute();
				lsns = results.asList();

				if (lsns.size() == 0) {
					result.message = "No entry found with with matching criteria";
				} else {
					getLogRegion().removeAll(lsns);
					result.message = "Successfully deleted " + lsns.size()
							+ " entries";
				}
				result.success = true;

			} catch (Exception e) {
				result.message = "Erorr enountered in delete -"
						+ e.getLocalizedMessage();
			}

		} else {
			result.message = "Invalid input";
		}
		return result;
	}
	
	/**
	 * @param inputdata
	 * @return
	 * @throws ClassNotFoundException
	 * This method deletes entries from mongodb database
	 */
	@POST
	@Path("/delete_db")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public ResultObject delete_db(InputData inputdata) throws ClassNotFoundException 
	{
		ResultObject resultObject = new ResultObject();
		List<Log> logs = new ArrayList();
		String output = "delete logs";
		String result;

		Boolean hasPayload = false;
		Boolean hasLogtype = false;
		Boolean hasLSN = false;
		Boolean hasStartTime = false;
		Boolean hasEndTime = false;
		String queryString = "";
		List<String> queries = new ArrayList<String>();
		String finalString = new String();
		if (inputdata != null) 
		{	
			Boolean addComma = false;
			if (inputdata.tid != null && inputdata.tid.toString().length() > 0) {
				queryString = queryString + "tid' = " + "'"
						+ inputdata.tid.toString() + "'";
				addComma = true;
				queries.add("tid");
				queries.add(inputdata.tid.toString());
			}
			else if(inputdata.appid != null && inputdata.appid.toString().length() > 0) {
				queryString = queryString + "appid' = " + "'"
						+ inputdata.appid.toString() + "'";
				addComma = true;
				queries.add("appid");
				queries.add(inputdata.appid.toString());

			}

			if (inputdata.lsn != null && inputdata.lsn.toString().length() > 0) {
				hasLSN = true;
				if(addComma == true)
				{
					queryString = queryString + ",";
				}
				queryString = queryString + "'lsn' = " + "'"
						+ inputdata.lsn.toString() + "'";
				queries.add("lsn");
				queries.add(inputdata.lsn.toString()  );
			}
			if (inputdata.logtype != null
					&& inputdata.logtype.toString().length() > 0) {
				hasLogtype = true;
				if(addComma == true)
				{
					queryString = queryString + ",";
				}
				queryString = queryString + "'logtype' = " + "'"
						+ inputdata.logtype.toString() + "'";
				queries.add("logtype");
				queries.add(inputdata.logtype.toString() );
			}
			String newPayloadJsonString =null;
			if(inputdata.payload !=null && inputdata.payload.length() > 0)
			{
				hasPayload = true;
				try
				{
					Object payLoadObject = SimpleUtil.XML_to_Object(inputdata.payload);
					String payloadJsonString = SimpleUtil.convert_object_to_JSON(payLoadObject);
					System.out.println(payloadJsonString);
					newPayloadJsonString = payloadJsonString.substring(1,payloadJsonString.length()-1 );

				} 
				catch (IllegalArgumentException  e)
				{
					e.printStackTrace();
				}
			}
			//TODO start time and end time

			String myJsonstring = "";
			Object object [] = queries.toArray();
			for (int i=0;i<object.length ;i+=2)
			{
				if(myJsonstring.length() > 0)
				{
					myJsonstring = myJsonstring + ",";
				}
				myJsonstring = myJsonstring + "\"" + (object[i].toString().replace("\"", "")) + "\"" + " : " + "\""+ object[i+1].toString().replace("\"", "") +"\"" ;

			}

			if(myJsonstring.length() > 0)
			{
				if(hasPayload == true)
				{
					finalString = "{ " +  myJsonstring + " , " + newPayloadJsonString + " }";
				}
				else
				{
					finalString = "{ " +  myJsonstring + "}";
				}
			}
			else
			{
				if(hasPayload == true)
				{
					finalString =  "{ " + newPayloadJsonString +" }";
				}

			}
		}
		int outputCount = DatabaseService.delete_db_object(finalString);
		
		if(outputCount ==-1)
		{
			resultObject.message = "Encountered error in deletion";
			resultObject.success = false;
		}
		else if(outputCount >0)
		{
			resultObject.message = "Successfully deleted entries";
			resultObject.success = true;
		}
		return resultObject;
	}


}
