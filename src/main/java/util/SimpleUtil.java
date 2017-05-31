package util;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.xml.sax.InputSource;

import service.GenericJson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SimpleUtil 
{
	public static Object XML_to_Object(String payload)
	{
		Object object = null;
		
			SAXBuilder saxBuilder = new SAXBuilder();
			org.jdom.Document doc = null;
			try {
				doc = saxBuilder.build(new StringReader(payload));
			} catch (JDOMException | IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				object = new Integer(100);
				return object;
			}
			String message = doc.getRootElement().getName();
			System.out.println("The root Element was " + message);
			String className = message.toUpperCase();
			String newClassName = "com.payload_XML.generatedclasses." + className.substring(0, 1) + message.substring(1);
			Class payloadclass;
			try {
				payloadclass = Class.forName(newClassName);
			} catch (ClassNotFoundException e) 
			{
				e.printStackTrace();
				object = new Integer(200);
				e.printStackTrace();
				return object;
			}
			JAXBContext jaxbContext = null;
			try 
			{	
				jaxbContext = JAXBContext.newInstance(payloadclass);
				Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
				InputSource inputsource = new InputSource(new StringReader(payload));
				object = jaxbUnmarshaller.unmarshal(inputsource);
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				object = new Integer(300);
				return object;
			}
		System.out.println("Created a object from XML");
		return object;
	}
	
	public static String convert_object_to_JSON(Object object)
	{
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		System.out.println(gson.toJson(object));
		return gson.toJson(object);
	}
	
	public static Object JSON_to_object(String payload)
	{
		Object object =null;
		if(payload !=null)
		{
			payload.replace("\"{", "{");
			payload.replace("}\"", "}");
		}
		try
		{
			System.out.println(payload);
			GsonBuilder builder = new GsonBuilder();
			Gson gson = builder.create();
			GenericJson genericJson = gson.fromJson(payload, GenericJson.class);
			Class className = Class.forName("com.payload_json.generatedclasses." + genericJson.type);
			object = gson.fromJson(genericJson.value, className);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			object = null;
		}
		return object;
	}

}
