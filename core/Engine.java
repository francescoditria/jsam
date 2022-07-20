package core;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.mysql.jdbc.Connection;

public class Engine {

	static Connection connection;
	static Map<String, Long> map = new HashMap<String,Long>();
	
    private String path;

	
	public void connect(String user,String pwd,String host,String port,String database){
		
		String jdbc="jdbc:mysql://"+host+":"+port+"/"+database;

        try {
			Class.forName("com.mysql.jdbc.Driver");
	        connection = (Connection) DriverManager.getConnection(jdbc, user, pwd);
	        System.out.println("Connected to "+host);
	        System.out.println("Using "+database);

	        this.extractTable(database);
	        	        
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();			
		}

	}
	

	
	
	public void extractTable(String database)
	{
		
		System.out.println("Extracting database profile");
        String[] types={"TABLE"};
		ResultSet rs;

		try {
			Statement statement=connection.createStatement();
	        String query="select TABLE_NAME, TABLE_ROWS from information_schema.TABLES where TABLE_SCHEMA='"+database+"'";
	        rs = statement.executeQuery(query);
			
	        map.clear();
            while (rs.next()) {
            	
            	String tableName=rs.getString("TABLE_NAME");
            	String tableRows=rs.getString("TABLE_ROWS");
            	long tr=Long.parseLong(tableRows);
            	map.put(tableName, tr);
            	
            }
		}catch (SQLException e) {
            e.printStackTrace();
	 }
		
	}
	
	public void exec(String function,String column,String table,String where)
	{
		double start_time = System.currentTimeMillis();

		String cwhere="";
		if(!where.isEmpty()) cwhere= " where "+where;

		
		long tr = map.get(table);
		double[] risp = new double[10];
		int g=10; //numero di iterate
		long n=tr / g;	//righe da estrarre ad ogni iterata
		long m=tr % g; //resto di righe da estrarre		
		int i=0;
		
		//System.out.println(tr+" "+g+" "+n+" "+m);
		String query="select "+function+"("+column+") from (select "+column+" from "+table+" LIMIT ";
		String query2;
		ResultSet rs;
		double parziale;
		double fattore;
		double end_time;
		double difference;
		double finale=0.0;
		double answ=0;
		
		long start;
		long end;
		try
		{
			Statement statement=connection.createStatement();
			for(i=0;i<g;i++) 				
			{
				start = n*i;
				end = n;
				if(i==g-1) end = end+m;
				query2=query+start+","+end+") as t"+cwhere;
				rs = statement.executeQuery(query2);
				rs.next();
				parziale=rs.getDouble(1);
				
				fattore=(double) g /(i+1);//(double) tr / ((tr / g) * (i+1));
				
				if(function.equals("sum") || function.equals("count")) 
		        {
					finale=(finale+parziale);
					answ=finale*fattore;
				}
				else
		        {
					parziale=(parziale * end); 
					finale=(finale+parziale);
					answ=(finale*fattore)/tr;
				
				}
				end_time = System.currentTimeMillis();
				difference = (end_time - start_time)/1000;
				//System.out.println("Approximate "+ function+ ": "+answ+ " ["+(i+1)*10+"%] "+difference+ " secs");
				System.out.println(answ+ " \t["+(i+1)*10+"%] \t"+difference+ " secs");		
				risp[i]=answ;
					
			}
	

			System.out.println("Statistics of the relative error");
			for(i=0;i<g;i++)
			{
				System.out.print(i+1+") ");
				System.out.printf("%.2f", Math.abs((risp[i]-answ)/answ)*100);
				System.out.print("%; ");
				
			}
			System.out.println();
			
			
		}catch (SQLException e) {e.printStackTrace();}
		
		
	}
	


}

	

