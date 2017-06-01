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
	public Log() 
	{
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
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((appid == null) ? 0 : appid.hashCode());
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + ((logtype == null) ? 0 : logtype.hashCode());
		result = prime * result + ((lsn == null) ? 0 : lsn.hashCode());
		result = prime * result + ((next == null) ? 0 : next.hashCode());
		result = prime * result + ((payload == null) ? 0 : payload.hashCode());
		result = prime * result + ((prev == null) ? 0 : prev.hashCode());
		result = prime * result + ((tid == null) ? 0 : tid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Log other = (Log) obj;
		if (appid == null) {
			if (other.appid != null)
				return false;
		} else if (!appid.equals(other.appid))
			return false;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (logtype == null) {
			if (other.logtype != null)
				return false;
		} else if (!logtype.equals(other.logtype))
			return false;
		if (lsn == null) {
			if (other.lsn != null)
				return false;
		} else if (!lsn.equals(other.lsn))
			return false;
		if (next == null) {
			if (other.next != null)
				return false;
		} else if (!next.equals(other.next))
			return false;
		if (payload == null) {
			if (other.payload != null)
				return false;
		} else if (!payload.equals(other.payload))
			return false;
		if (prev == null) {
			if (other.prev != null)
				return false;
		} else if (!prev.equals(other.prev))
			return false;
		if (tid == null) {
			if (other.tid != null)
				return false;
		} else if (!tid.equals(other.tid))
			return false;
		return true;
	}


}
