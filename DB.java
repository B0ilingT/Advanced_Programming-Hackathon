package uk.ac.mmu.advprog.hackathon;

import org.json.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Handles database access from within your web service
 * @author You, Mainly!
 */
public class DB implements AutoCloseable 
{
	
	//allows us to easily change the database used
	private static final String JDBC_CONNECTION_STRING = "jdbc:sqlite:./data/AMI.db";
	
	//allows us to re-use the connection between queries if desired
	protected Connection connection = null;
	
	/**
	 * Creates an instance of the DB object and connects to the database
	 */
	public DB()
	{
		try 
		{
			connection = DriverManager.getConnection(JDBC_CONNECTION_STRING);
		}
		catch (SQLException sqle) 
		{
			error(sqle);
		}
	}
	
	/**
	 * Returns the number of entries in the database, by counting rows
	 * @return The number of entries in the database, or -1 if empty
	 */
	public int getNumberOfEntries() 
	{
		int result = -1;
		try 
		{
			Statement s = connection.createStatement();
			ResultSet results = s.executeQuery("SELECT COUNT(*) AS count FROM ami_data");
			while(results.next()) 
			{ //will only execute once, because SELECT COUNT(*) returns just 1 number
				result = results.getInt(results.findColumn("count"));
			}
		}
		catch (SQLException sqle) 
		{
			error(sqle);			
		}
		return result;
	}
	/**
	 * 
	 * @param urlString
	 * This Method is used to find the last signal displayed on a particular sign and uses a prepared statement, s a parametrised query which
	 * is used to protect my web service from SQL Injection attacks. The SQL Query selects the specific signal id from the database, checks its not
	 * 'OFF' 'NR' or 'BLNK' and then finds the last displayed signal and returns it as a string to the lastsignal route by extracting it from the results.
	 * @return
	 */
	public String lastSignal(String urlString)
	{
			String lastSignal = "no results"; 
			try
			{		
				PreparedStatement s;
				s = connection.prepareStatement("SELECT signal_value FROM ami_data"
						+ " WHERE signal_id = ?"
						+ " AND NOT signal_value = 'OFF'"
						+ " AND NOT signal_value = 'NR' "
						+ " AND NOT signal_value = 'BLNK'"
						+ " ORDER BY datetime DESC"
						+ " LIMIT 1;");
				s.setString(1, urlString);
				ResultSet results = s.executeQuery();
				while (results.next())
				{
					lastSignal = results.getString(results.findColumn("signal_value"));
				}
			}
			catch (SQLException sqle)
			{
				error(sqle);
			}				
			return lastSignal;			
	}
	/**
	 * 
	 * @param urlString
	 * This method is used to find the occurrence of each signals on the entire motorway,
	 * this method accepts a shortened url from the route and returns a single line list of each signal and the frequency of which they occurred.
	 * @return
	 */
	public JSONArray frequency(String urlString)
	{	
		JSONArray freqArray = new JSONArray(); 	
		int freq = 0;
		try
		{	
			PreparedStatement s;
			s = connection.prepareStatement("SELECT"
					+ " COUNT(signal_value) AS frequency,"
					+ " signal_value"
					+ " FROM ami_data"
					+ " WHERE signal_id LIKE ?"
					+ " GROUP BY signal_value"
					+ " ORDER BY frequency DESC;");	
			s.setString(1, urlString + "%");
			ResultSet results = s.executeQuery();
			while (results.next())
				{
					freq = results.getInt(results.findColumn("frequency"));
					String sigVal = results.getString(results.findColumn("signal_value"));
					JSONObject frequencyObj = new JSONObject();
					frequencyObj.put("Value: " + sigVal,"Frequency: " + freq);
					freqArray.put(frequencyObj);
				}
		}
		catch (SQLException sqle)
		{
			error(sqle);
		}	
		return freqArray;
	}
	/**
	 * This method accepts a signal group and date/time parameter, it returns an XML data structure with the signal groups and date/time and the values at that time.
	 * @param sigGroup
	 * @param dt
	 * @return
	 */
	public String signalsattime(String sigGroup, String dt)
	{
		Writer output = new StringWriter();
		String xmlString ="";
		try
		{
			PreparedStatement s;
			s = connection.prepareStatement("SELECT datetime, signal_id, signal_value"
					+ " FROM ami_data "
					+ " WHERE "
					+ " signal_group = ?" //M6-J4-4A
					+ " AND datetime < ?" //2021-05-20 14:50
					+ " AND (datetime, signal_id) IN ( "
					+ " SELECT MAX(datetime) AS datetime, signal_id"
					+ " FROM ami_data "
					+ " WHERE "
					+ " signal_group = ? " //M6-J4-4A
					+ " AND datetime < ? " //2021-05-20 14:50
					+ " GROUP BY signal_id"
					+ ")"
					+ " ORDER BY signal_id;");
			s.setString(1, sigGroup);
			System.out.println(sigGroup);
			s.setString(2, dt);
			System.out.println(dt);
			s.setString(3, sigGroup);
			s.setString(4, dt);
			ResultSet results = s.executeQuery();
			
			DocumentBuilderFactory cheese = DocumentBuilderFactory.newInstance();
			Document doc = cheese.newDocumentBuilder().newDocument();			
			Element Signals = doc.createElement("Signals");
			doc.appendChild(Signals);
			while (results.next())
			{
				Element signal1 = doc.createElement("Signal");
				Element id1 = doc.createElement("ID");
				id1.setTextContent(results.getString(results.findColumn("signal_group")));
				Element dateset1 = doc.createElement("DateSet");
				dateset1.setTextContent(results.getString(results.findColumn("datetime")));
				Element value1 = doc.createElement("Value");
				value1.setTextContent(results.getString(results.findColumn("signal_value")));
				Signals.appendChild(signal1); 
				Signals.appendChild(id1);
				Signals.appendChild(dateset1);
				Signals.appendChild(value1);
			}
			Transformer transformer = TransformerFactory.newInstance().newTransformer();			
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(new DOMSource(doc),new StreamResult(output));
			StreamResult result = new StreamResult(new StringWriter());
			DOMSource source = new DOMSource(doc);
			transformer.transform(source, result);
			xmlString = result.getWriter().toString();
		}		
		catch (SQLException sqle)
		{
			error(sqle);
		}	
		catch (ParserConfigurationException | TransformerException ioe)
		{
			System.out.println("Error creating XML: " + ioe);
		}	
		return xmlString;
	}
	/**
	 * This Method returns an XML data list of all the motorway groups in the database, it accepts no parameters.
	 * @return
	 */
	public String groups()
	{
		Writer output = new StringWriter();
		String xmlString ="";
		try
		{
			PreparedStatement s;
			s = connection.prepareStatement("SELECT DISTINCT signal_group FROM ami_data;");
			ResultSet results = s.executeQuery();
			
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			Document doc = dbf.newDocumentBuilder().newDocument();			
			Element Groups = doc.createElement("Groups");
			doc.appendChild(Groups);
			
			while (results.next())
			{
				Element group1 = doc.createElement("Group");
				group1.setTextContent(results.getString(results.findColumn("signal_group")));
				Groups.appendChild(group1);
			}
			Transformer transformer = TransformerFactory.newInstance().newTransformer();			
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(new DOMSource(doc),new StreamResult(output));
			StreamResult result = new StreamResult(new StringWriter());
			DOMSource source = new DOMSource(doc);
			transformer.transform(source, result);
			xmlString = result.getWriter().toString();
		}
		catch (SQLException sqle)
		{
			error(sqle);
		}	
		catch (ParserConfigurationException | TransformerException ioe)
		{
			System.out.println("Error creating XML: " + ioe);
		}	
		return xmlString;
	}
	
	/**
	 * Closes the connection to the database, required by AutoCloseable interface.
	 */
	@Override
	public void close() 
	{
		try 
		{
			if ( !connection.isClosed() ) 
			{
				connection.close();
			}
		}
		catch(SQLException sqle) 
		{
			error(sqle);
		}
	}

	/**
	 * Prints out the details of the SQL error that has occurred, and exits the programme
	 * @param sqle Exception representing the error that occurred
	 */
	protected void error(SQLException sqle) 
	{
		System.err.println("Problem Opening Database! " + sqle.getClass().getName());
		sqle.printStackTrace();
		System.exit(1);
	}
}
