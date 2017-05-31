package service;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;

/**
 * @author reeta
 * This is the class that will be stored in database and returned back to sure on serahc query
 */
public class Log  implements PdxSerializable
{
	
	public Log() {
		// TODO Auto-generated constructor stub
	}

	public String tid;
	public String lsn;
	public String next;
	public String prev;
	public Date date;
	public String logtype;
	public String payload;
	public String appid;
	
	
	public Log(String tid, String lsn, String next, String prev, Date date,
			String type, String payload, String appid) {
		super();
		this.tid = tid;
		this.lsn = lsn;
		this.next = next;
		this.prev = prev;
		this.date = date;
		this.logtype = type;
		this.payload = payload;
		this.appid = appid;
	}

	@Override
	public String toString()
	{
	        return "log [ tid=" + tid +", lsn=" + lsn + ", payload= " + payload + ", date" + date + "]";
	}

	@Override
	public void toData(PdxWriter writer) 
	{
		writer.writeString("tid", tid);
		writer.writeString("lsn", lsn);
		writer.writeString("next", next);
		writer.writeString("prev", prev);
		writer.writeDate("date", date);
		writer.writeString("logtype", logtype);
		writer.writeString("payload", payload);
		writer.writeString("appid", appid);
		writer.markIdentityField("lsn");
	}

	@Override
	public void fromData(PdxReader reader) 
	{	
		try
		{
		System.out.println("reached here");
		tid = reader.readString("tid");
		lsn = reader.readString("lsn");
		next = reader.readString("next");
		prev = reader.readString("prev");
		date = reader.readDate("date");
		logtype = reader.readString("logtype");
		payload = reader.readString("payload");
		appid = reader.readString("appid");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}


}
