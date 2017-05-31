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
	public UUID tid;
	public String payload;
	public String logtype;
	public UUID lsn;
	public Date starttime;
	public Date endtime;
}

