package core;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.DatabaseMetaData;

public class Engine {

	static Connection connection;
	static Map<String, Long> map = new HashMap<String,Long>();
	static Map<String, Double> colMap = new HashMap<String,Double>();

    private String path;

	
	public void connect(String user,String pwd,String host,String port,String database){
		
		String jdbc="jdbc:mysql://"+host+":"+port+"/"+database;

        try {
			Class.forName("com.mysql.jdbc.Driver");
	        connection = (Connection) DriverManager.getConnection(jdbc, user, pwd);
	        System.out.println("Connected to "+host);
	        System.out.println("Using "+database);

	        this.extractTable(database);
	        System.out.println("Model built");
	        	        
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();			
		}

	}
	

	private void getColumns(String tableName,String database) throws SQLException
	{
		
		System.out.println("Extacting columns in "+tableName);
		ResultSet ds;

		ds = connection.getMetaData().getColumns(database, null,tableName, null);
	    while (ds.next()) {
         	String columnName=ds.getString("COLUMN_NAME");
         	String columnType=ds.getString("TYPE_NAME");
            //System.out.println(columnName);
         	if(this.isSafeType(columnType) && !this.isPK(tableName, columnName) && !this.isFK(tableName, columnName))
         	{
         		this.getSummary(tableName, columnName);
         	}
            
	    }
	}

	
	private void getSummary(String tableName, String columnName)  throws SQLException
	{
		System.out.println("Computing summaries of "+columnName +" in "+tableName);
		String query="select min("+columnName+") as min,max("+columnName+") as max,round(avg("+columnName+"),2) as avg,round(stddev("+columnName+"),2) as stddev from "+tableName+" where "+columnName+" is not null";
		String y[] = new String[4];
        Statement statement=connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        rs.next();
        //System.out.println(rs.getString("n"));
        y[0]=rs.getString("min");
        y[1]=rs.getString("max");
        y[2]=rs.getString("avg");
        y[3]=rs.getString("stddev");

        double avg=0;
        double stddev=0;
        double cf;
        if(y[2]!=null)
        	avg=Double.parseDouble(y[2]);
        if(y[3]!=null)
            stddev=Double.parseDouble(y[3]);
        //System.out.println(y[0]+"\t"+y[1]+"\t"+y[2]+"\t"+y[3]);
        if(avg!=0)
        	cf=stddev/avg;
        else
        	cf=0;
        //double cf2;
        //double min=0.1;
        //double max=1;
        //double min2=10;
        //double max2=90;
        
        //if(cf<=min) cf2=min2;
        //else if(cf>=max) cf2=max2;
        //else cf2=(cf-min)/(max-min)*(max2-min2)+min2;
        
        //double factor=100/cf2;
        //System.out.println(tableName+"."+columnName+"\tAVG "+y[2]+"\tSTD "+y[3]+"\tCF "+cf+"\tCF2 "+cf2+"\tFact "+factor);
        colMap.put(tableName+"."+columnName, cf);
        
        
	}
	
	
	private boolean isSafeType(String type)
	{
		type=type.toUpperCase();
		int i;
		String[] safe={"INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT","DECIMAL", "NUMERIC","FLOAT","DOUBLE"};
		
		int n=safe.length;
		
		for(i=0;i<n;i++)
		{
			if(type.equals(safe[i]))
					return true;
		}
		
		return false;
	}


	private boolean isPK(String tableName,String columnName) throws SQLException
	{
		DatabaseMetaData meta;
	 	meta = (DatabaseMetaData) connection.getMetaData();
	 	ResultSet rs=meta.getPrimaryKeys(null, null, tableName);
	 	while(rs.next())
	 	{
	 		String pk= rs.getString(4);
	 		//System.out.println(pk);
	 		if(columnName.equals(pk))
	 		{
	 			return true;
	 		}
	 	}

		return false;
	}

	
	private boolean isFK(String tableName,String columnName) throws SQLException
	{
		DatabaseMetaData meta;
	 	meta = (DatabaseMetaData) connection.getMetaData();
	 	ResultSet rs=meta.getImportedKeys(null, null, tableName);
	 	while(rs.next())
	 	{
	 		String fk= rs.getString(8);
	 		//System.out.println(fk);
	 		if(columnName.equals(fk))
	 		{
	 			return true;
	 		}
	 	}

		return false;
	}

	
	public void extractTable(String database)
	{
		
		System.out.println("Extracting database profile");
        String[] types={"TABLE"};
		ResultSet rs;

		try {
			Statement statement=connection.createStatement();
	        String query="select TABLE_NAME, TABLE_ROWS from information_schema.TABLES where TABLE_SCHEMA='"+database+"' and TABLE_TYPE='BASE TABLE'";
	        rs = statement.executeQuery(query);
			
	        map.clear();
            while (rs.next()) {
            	
            	String tableName=rs.getString("TABLE_NAME");
            	//System.out.println(tableName);
            	String tableRows=rs.getString("TABLE_ROWS");
            	long tr=Long.parseLong(tableRows);
            	map.put(tableName, tr);
            	this.getColumns(tableName,database);
                
            	
            }
		}catch (SQLException e) {
            e.printStackTrace();
	 }
		
	}
	
	public void execPA(String function,String column,String table,String where)
	{
		//Progressive Accuracy
		
		double start_time = System.currentTimeMillis();

		String cwhere="";
		if(!where.isEmpty()) cwhere= " where "+where;

		
		long tr = map.get(table);
		double cf=colMap.get(table+"."+column);
		//System.out.println("cf "+cf);
		//System.out.println("Coefficient of variation: "+cf);
		//determinare il numero di iterate g in base al cf
		//determinare il confidence degree in base al cf e alla percentuale di campionamento
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
				
				int percent=(i+1)*10;//sample percentage
				if(cf>1) cf=1; //coefficient of variation
				double cd=Math.max(percent,100-cf*100); //confidence degree
				//System.out.println("Approximate "+ function+ ": "+answ+ " ["+(i+1)*10+"%] "+difference+ " secs");
				System.out.println(answ+ " \t["+percent+"%]\t["+cd+"%]\t"+difference+ " secs");		
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
	

	public void execAS(String function,String column,String table,String where)
	{
		//Adaptive Sampling
		
		double start_time = System.currentTimeMillis();

		String cwhere="";
		if(!where.isEmpty()) cwhere= " where "+where;
		
		long tr = map.get(table);
		double cf=colMap.get(table+"."+column);
		if(cf>1) cf=1;
		
		double cf2;
        double min=0.1;
        double max=0.9;
        double min2=10;
        double max2=90;
        if(cf<=min) cf2=min2;
        else if(cf>=max) cf2=max2;
        else cf2=(cf-min)/(max-min)*(max2-min2)+min2;        
        double factor=100/cf2;      
        long n=(long) (tr/factor);	//righe da estrarre
		//System.out.println("N="+n+"/"+tr);
		
        String query2="select "+function+"("+column+") from (select * from "+table+cwhere+" LIMIT 0,"+n+") as t";
		
		ResultSet rs;
		Double result;
		double finale;		
		double final_time;
		double difference;

		
        Statement statement;
		try {
			statement = connection.createStatement();
			rs = statement.executeQuery(query2);
			rs.next();
			result=rs.getDouble(1);
			finale=result;
			if(function.equals("sum") || function.equals("count")) 
	        {
				finale=factor*result;
			}

			final_time = System.currentTimeMillis();
			difference = (final_time - start_time)/1000;
			System.out.println(Math.round(finale)+"\t["+difference+" secs]");

			//double xx=cf2/100;
			//System.out.println(xx);
			//double yy=Math.abs(finale-finale/(1-xx));
			//System.out.println("+-"+yy+" ["+cf*100+"%]");
			double xx=100-cf2;
			double perc=finale/100*10;
			//System.out.println(perc);
			System.out.println("["+(finale-perc)+", "+(finale+perc)+"] 90%");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

		

}

	

