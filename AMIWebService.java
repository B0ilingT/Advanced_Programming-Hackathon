package uk.ac.mmu.advprog.hackathon;
import static spark.Spark.get;
import static spark.Spark.port;

import spark.Request;
import spark.Response;
import spark.Route;

/**
 * Handles the setting up and starting of the web service
 * You will be adding additional routes to this class, and it might get quite large
 * Feel free to distribute some of the work to additional child classes, like I did with DB
 * @author You, Mainly!
 */
public class AMIWebService 
{

	/**
	 * Main program entry point, starts the web service
	 * @param args not used
	 */
	public static void main(String[] args) 
	{		
		port(8088);
		
		//Simple route so you can check things are working...
		//Accessible via http://localhost:8088/test in your browser
		get("/test", new Route() 
		{
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{
					return "Number of Entries: " + db.getNumberOfEntries();
				}
			}
			
		});
		
		get("/lastsignal", new Route() 
		{
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{	
					String url = request.queryParams("signal_id");
					return "Last Signal was: " + db.lastSignal(url) ;					
				}
			}			
		});	
		
		get("/frequency", new Route() 
		{
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{	
					response.type("application/json");
					String url = request.queryParams("motorway");
					return "Frequency of signals displayed on " + url + db.frequency(url);
				}
			}			
		});	
		
		get("/groups", new Route() 
		{
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{	
					response.type("application/xml");
					return db.groups();
				}
			}			
		});
		
		get("/signalsattime", new Route() 
		{
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{	
					response.type("application/xml");
					String urlg = request.queryParams("group");
	                String sigGroup = urlg.substring(urlg.indexOf("=") + 1);
	                sigGroup  = sigGroup.substring(0, sigGroup.indexOf("&"));
					String dt = request.queryParams("time");
					dt = dt.replaceAll("+", " ");
					return db.signalsattime(sigGroup, dt);
				}
			}			
		});
		
		get("/signalsattime", new Route() 
		{
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{	
					String url = request.queryParams("motorway");
					return "Most frequently displayed Signals on" + url + db.frequency(url);
				}
			}			
		});		
		System.out.println("Server up! Don't forget to kill the program when done!");
	}

}
