package service;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.simple.JSONObject;
import org.apache.geode.DataSerializable;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.*;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;





import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

import com.sun.jersey.spi.resource.Singleton;


 
@Path("/logging")
@Singleton
public class LoggingService 
{
	public static ClientCache cache;
	public static Region<String, String> transactionRegion;
	public static Region<String, Log> logRegion;
	
	
	public LoggingService() {
		super();
		cache= null;
		transactionRegion = null;
		logRegion = null;
	}
	public static ClientCache getCache(){
		if(cache==null){
			cache = new ClientCacheFactory().setPdxSerializer(new ReflectionBasedAutoSerializer("Log")).addPoolLocator("localhost", 10334).create();
		}
		return cache;
		
	}
	
	public static Region<String,String> getTransactionRegion()
	{
		if (transactionRegion == null)
		{
			transactionRegion = getCache().<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).setValueConstraint(String.class).setKeyConstraint(String.class).create("transaction");
		}
		return transactionRegion;
	}
	
	public static Region<String,Log> getLogRegion()
	{
		if (logRegion == null)
		{
			logRegion = getCache().<String, Log> createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).setValueConstraint(Log.class).setKeyConstraint(String.class).create("log");
		}
		return logRegion;
	}
	
	@GET
	@Path("/begin")
	@Produces(MediaType.APPLICATION_JSON)
	public JSONObject getTid()
	{
		String tid = java.util.UUID.randomUUID().toString();
		try
		{
			getTransactionRegion().put(tid.toString(),"");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			tid = "Unable to create transaction";
		}
		//return Response.status(200).entity(output).build();
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("tid", tid);
		return jsonObject;
	}
	
	@POST
	@Path("/write")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public JSONObject write_log(InputData inputdata)
	{
		ResultObject result = create_log(inputdata);
		
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("message", result.message);

		if(result.success == true)
		{
			jsonObject.put("status", "Success");
		}
		else
		{
			jsonObject.put("status", "Failed");
		}
		
		return jsonObject;
	}
	
	
	private ResultObject create_log(InputData inputdata)
	{
		ResultObject result = new ResultObject();
		Boolean isTransaction = false;
		if(inputdata != null)
		{
				if (inputdata.payload == null)
				{
					result.message = "Payload absent";
					return result;
				}
				else if(inputdata.logtype == null)
				{
					
				}
				if(inputdata.tid != null)
				{
					isTransaction = true;
					try
					{
						if (getTransactionRegion().get(inputdata.tid.toString()) == null)
						{
							result.message = "Invalid Transaction ID";
							return result;
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
						result.message = "Encountered error in creating Log";
						return result;
					}
				}
				
					
			UUID lsn = java.util.UUID.randomUUID();
			
			Log log = null;
			if (isTransaction == false)
			{
				log = new Log("",lsn.toString(),"","",new Date(),inputdata.logtype, inputdata.payload);
			}
			else
			{
				log = new Log(inputdata.tid.toString(),lsn.toString(),"","",new Date(),inputdata.logtype, inputdata.payload);
			}	
				
			try
			{
				getLogRegion().put(lsn.toString(),log);
				System.out.println("Data saved");
			}
			catch(Exception e)
			{
				e.printStackTrace();
				result.message = "Encountered error in saving log";
				return result;
			}
			
			result.message = lsn.toString();
			result.success = true;
			return result;
		}
		else
		{
			result.message = "Input cannot be null or blank";
			return result;
		}
		
	}
	
	@DELETE
	@Path("/delete")
	public ResultObject delete_log(InputData inputdata)
	{
		ResultObject result = new ResultObject();
		result.success = false;
		List<String> lsns = new ArrayList();
		
		Boolean hasPayload = false;
		Boolean hasLogtype = false;
		Boolean hasLSN = false;
		Boolean hasStartTime = false;
		Boolean hasEndTime = false;
				
		if(inputdata != null)
		{
			String queryString = "select v.lsn from /log.values v where 1=1";
			
			if (inputdata.tid !=null && inputdata.tid.toString().length() > 0 )
			{
				queryString = queryString + " and v.tid = " + "'" + inputdata.tid.toString() +"'";
			}
			
			if (inputdata.lsn !=null && inputdata.lsn.toString().length() > 0 )
			{
				hasLSN = true;
				queryString = queryString + " and v.lsn = " + "'" + inputdata.lsn.toString() +"'";
			}
			if(inputdata.logtype != null && inputdata.logtype.toString().length() > 0)
			{
				hasLogtype = true;
				queryString = queryString + " and v.logtype = " + "'" + inputdata.logtype.toString()+"'";
			}
			if((inputdata.starttime != null && inputdata.starttime.toString().length() > 0 ) && (inputdata.endtime != null && inputdata.endtime.toString().length() > 0 ))
			{
				hasStartTime = true;
				queryString = queryString + " and v.date between " + inputdata.starttime.toString() + " and " + inputdata.endtime.toString();
			}
			if (inputdata.payload != null && inputdata.payload.length() > 0)
			{
					hasPayload = true;	
					queryString = queryString + " and v.payload = " + "'" + inputdata.payload.toString() +"'";
			}
			if (queryString.equals("select * from /log.values v where 1=1"))
			{
				result.message = "Give some criteria to delete log";
				return result;
			}
			
			System.out.println(queryString);
			
			QueryService queryService = getCache().getQueryService();

			
			Query query = queryService.newQuery(queryString);
			 
			
			SelectResults results;
			try {
				results = (SelectResults)query.execute();
				lsns = results.asList(); 

				if(lsns.size() ==0)
				{
					result.message ="No entry found with with matching criteria";
				}
				else
				{
					getLogRegion().removeAll(lsns); 
					result.message ="Successfully deleted " + lsns.size() + " entries";
				}
				result.success = true;
				
			}
			catch (Exception e)
			{
				e.printStackTrace();
				result.message ="Erorr enountered in delete -" + e.getLocalizedMessage();
			}

		}
		else
		{
			result.message = "Invalid input";
		}
		return result;		
	}
	
	@PUT
	@Path("/flush")
	public String flush_log()
	{
		String output = "flush logs";
		//return Response.status(200).entity(output).build();
		return output;
	}
	
	@POST
	@Path("/query")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public List<Log> query_log(InputData inputdata)
	{
		List<Log> logs = new ArrayList();
		String output = "query logs";
		String result;
		
		Boolean hasPayload = false;
		Boolean hasLogtype = false;
		Boolean hasLSN = false;
		Boolean hasStartTime = false;
		Boolean hasEndTime = false;
				
		if(inputdata != null)
		{
			String queryString = "select * from /log.values v where 1=1";
			
			if (inputdata.tid !=null && inputdata.tid.toString().length() > 0 )
			{
				queryString = queryString + " and v.tid = " + "'" + inputdata.tid.toString() +"'";
			}
			
			if (inputdata.lsn !=null && inputdata.lsn.toString().length() > 0 )
			{
				hasLSN = true;
				queryString = queryString + " and v.lsn = " + "'" + inputdata.lsn.toString() +"'";
			}
			if(inputdata.logtype != null && inputdata.logtype.toString().length() > 0)
			{
				hasLogtype = true;
				queryString = queryString + " and v.logtype = " + "'" + inputdata.logtype.toString()+"'";
			}
			if((inputdata.starttime != null && inputdata.starttime.toString().length() > 0 ) && (inputdata.endtime != null && inputdata.endtime.toString().length() > 0 ))
			{
				hasStartTime = true;
				queryString = queryString + " and v.date between " + inputdata.starttime.toString() + " and " + inputdata.endtime.toString();
			}
			if (inputdata.payload != null && inputdata.payload.length() > 0)
			{
					hasPayload = true;	
					queryString = queryString + " and v.payload = " + "'" + inputdata.payload.toString() +"'";
			}
			if (queryString.equals("select * from /log.values v where 1=1"))
			{
				return logs;
			}
			
			System.out.println(queryString);
			
			QueryService queryService = getCache().getQueryService();

			
			Query query = queryService.newQuery(queryString);
			 
			
			SelectResults results;
			try {
				results = (SelectResults)query.execute();
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
		}
		else
		{
			return logs;
		}
	}
	
	@POST
	@Path("/rollback")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public ResultObject rollback_logs(InputData inputData)
	{
		ResultObject result = new ResultObject();
		result.success = false;
		
		if(inputData.tid != null)
		{
			try
			{
				if (getTransactionRegion().get(inputData.tid.toString()) == null)
				{
					result.message = "Invalid transaction ID";
				}
				String queryString = "select v.lsn from /log.values v where v.tid = " + "'" + inputData.tid.toString() + "'";
				System.out.println(queryString);
				
				QueryService queryService = getCache().getQueryService();

				
				Query query = queryService.newQuery(queryString);
				 
				
				SelectResults results;
				List<String> lsns = new ArrayList();
				try 
				{
					results = (SelectResults)query.execute();
					System.out.println(results.size());
					lsns = results.asList();
					if(lsns.size() ==0)
					{
						result.message ="No entry found with tid - " + inputData.tid.toString();
					}
					else
					{
						getLogRegion().removeAll(lsns);
						getTransactionRegion().remove(inputData.tid.toString()); 
						result.message ="Successfully rolledback";
					}
					result.success = true;
					
				} catch (Exception e)
				{
					e.printStackTrace();
					result.message ="Faced error in rollback -" + e.getLocalizedMessage();
				}
				
				
			}
			catch(Exception e)
			{
				e.printStackTrace();
				result.message ="Faced error in rollback -" + e.getLocalizedMessage();
			}
		}
		else
		{
			result.message ="Invalid input";
			
		}
		return result;
	}
	
	@POST
	@Path("/commit")
	public String commit_logs(String tid)
	{
		if(tid != null)
		{
			try
			{
				
				if (getTransactionRegion().get(tid) == null)
				{
					return "Invalid Transaction id";
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		String output = "commit logs";
		//return Response.status(200).entity(output).build();
		return output;
	}
	
	
}

