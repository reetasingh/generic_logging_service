package service;

import java.sql.Date;
import java.util.UUID;

/**
 * @author reeta
 * This is the data that will be give to the methods for writing, searching, deleting logs
 */
public class InputData
{
	public String appid ;
	public String tid;
	public String payload;
	public String logtype;
	public String lsn;
	public Date starttime;
	public Date endtime;
	public InputData(String appid, String tid, String payload, String logtype,
			String lsn, Date starttime, Date endtime) {
		super();
		this.appid = appid;
		this.tid = tid;
		this.payload = payload;
		this.logtype = logtype;
		this.lsn = lsn;
		this.starttime = starttime;
		this.endtime = endtime;
	}
	public InputData() {
		super();
	}
	
	
}

