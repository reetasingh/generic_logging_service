package util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;

/**
 * @author reeta
 *
 */
public class SequenceGenerator 
{
	static AtomicInteger lsn=null;
	static AtomicInteger tid=null;
	static String lsnValue = null;
	static String tidValue = null;
	static OutputStream output = null;

	public  static void initalize()
	{
		org.apache.commons.configuration.PropertiesConfiguration prop;
		try {
			System.out.println("Initializing sequence generator");
			prop = new org.apache.commons.configuration.PropertiesConfiguration("config.properties");
			// get the property value and print it out
			System.out.println("lsn initial value - " + prop.getProperty("lsn"));
			lsnValue = (String) prop.getProperty("lsn");
			lsn = new AtomicInteger(new Integer (lsnValue));
			System.out.println("tid initial value - " + prop.getProperty("tid"));
			tidValue = (String) prop.getProperty("tid");
			tid = new AtomicInteger(new Integer(tidValue));
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public  static synchronized String getLsn()
	{
		try
		{
			org.apache.commons.configuration.PropertiesConfiguration conf = new org.apache.commons.configuration.PropertiesConfiguration("config.properties");
			conf.setProperty("lsn", (new Integer (lsn.incrementAndGet())).toString());
			conf.save(); 
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		return new Integer (lsn.get()).toString();
	}
	
	public  static synchronized String getTid() 
	{
		try
		{
			org.apache.commons.configuration.PropertiesConfiguration conf = new org.apache.commons.configuration.PropertiesConfiguration("config.properties");
			conf.setProperty("tid", (new Integer (tid.incrementAndGet())).toString());
			conf.save(); 
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return new Integer (tid.get()).toString();
	}
	
	public static void main(String args[])
	{
		SequenceGenerator sn = new SequenceGenerator();
		sn.initalize();
		System.out.println(sn.getTid());
		System.out.println(sn.getTid());
	}
}
