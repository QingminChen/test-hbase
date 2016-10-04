package com.qingmin.test.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

//import javax.security.auth.login.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 *
 */
public class CRDUApp {
	
	private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
	  private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";
	  
    public static void main( String[] args ){
    	
    	String a = "C:\\GitHubWorkspace\\test.hbase\\src\\mainresources\\core-site.xml";
    	String b = "C:\\GitHubWorkspace\\test.hbase\\src\\mainresources\\hbase-site.xml";
    	String c = "C:\\GitHubWorkspace\\test.hbase\\src\\mainresources\\hdfs-site.xml";
    	String d = "C:\\GitHubWorkspace\\test.hbase\\src\\mainresources\\mapred-site.xml";
    	String e = "C:\\GitHubWorkspace\\test.hbase\\src\\mainresources\\yarn-site.xml";
    	String f = "C:\\GitHubWorkspace\\test.hbase\\src\\mainresources\\slave";
    	
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(a);
        configuration.addResource(b);
        configuration.addResource(c);
        configuration.addResource(d);
        configuration.addResource(e);
        configuration.addResource(f);
        
        /**
         * configuration.set("hbase.zookeeper.quorum", "hadoop-spark-kafka.test.com");
         * configuration.set("hbase.rootdir", "hdfs://hadoop-spark-kafka.test.com:8020/user/sparkkafkatest/hbase");
         * */
        //createSchemaTables(configuration);
        //addColumnFamily(configuration);
        //modifySchema(configuration);     //这里版本似乎有问题，因为源码中列族的所有信息查出来是空，导致我传什么正确的数据都判断不存在,因为HTableDescriptor有问题，导致families一直取不到为空
        //deleteSchema(configuration); 
        //allMethodSchema(configuration);
        
        listSchemaTables(configuration);

	
        
        
    }
    
    public static void listSchemaTables(Configuration config){
    	TableName tableName = TableName.valueOf(TABLE_NAME);
    	Connection connection;
    	try {
			connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
	    	HTableDescriptor[] listTables = admin.listTables();
	    	for (HTableDescriptor hTableDescriptor : listTables){
	    		if(hTableDescriptor.getNameAsString().equals(TABLE_NAME)){
	    			//HColumnDescriptor[] listCF =hTableDescriptor.getColumnFamilies();
	    			HTableDescriptor table =  new HTableDescriptor(hTableDescriptor);
	    			System.out.print("modify OldCF. ");
	    	          // Update existing column family
	    	            //HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
	    	            HColumnDescriptor existingColumn = table.getFamily(Bytes.toBytes(CF_DEFAULT));
	    	            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
	    	            table.modifyFamily(existingColumn);
	    	            
	    	            admin.modifyTable(tableName, table);
	    	            admin.close();
	    	            System.out.print("modify done. ");

	    			
	    		}
	    		
	    	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static void addOneRecord(Configuration config){
    	TableName tableName = TableName.valueOf(TABLE_NAME);
    	Connection connection;
    	try {
			connection = ConnectionFactory.createConnection(config);   //HTablePool源码说要用HConnectionManager，如下:@deprecated as of 0.98.1. See {@link HConnection#getTable(String)}.，HConnectionManager源码提示说要用ConnectionFactory@deprecated Please use ConnectionFactory instead，这里的ConnectionFactory就是0.98版本的HTablePool
			Admin admin = connection.getAdmin();
			HTablePool hTablePool = new HTablePool();
	    	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static void createSchemaTables(Configuration config){
        Connection connection;
		try {
			connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
	        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
	        table.addFamily(new HColumnDescriptor(CF_DEFAULT));

	        System.out.print("Creating table. ");
	        admin.createTable(table);
	        System.out.println(" Done.");
	        admin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
   }
    
    public static void addColumnFamily(Configuration config){
    	Connection connection;
		try {
			connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
	            System.out.println("Table does not exist.");
	            System.exit(-1);
            }

            System.out.print("add NewCF. ");
          // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);
            admin.close();
            System.out.print("add done. ");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    public static void modifySchema(Configuration config) {
        Connection connection;
		try {
			connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
	            System.out.println("Table does not exist.");
	            System.exit(-1);
            }

            HTableDescriptor table = new HTableDescriptor(tableName);

            System.out.print("modify OldCF. ");
          // Update existing column family
            //HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
            HColumnDescriptor existingColumn = table.getFamily(Bytes.toBytes(CF_DEFAULT));
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);
            
            admin.modifyTable(tableName, table);
            admin.close();
            System.out.print("modify done. ");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
      }
    
    public static void deleteSchema(Configuration config){
    	 Connection connection;
 		try {
 			connection = ConnectionFactory.createConnection(config);
 			Admin admin = connection.getAdmin();
 			
 			TableName tableName = TableName.valueOf(TABLE_NAME);
 			
	        // Disable an existing table
 			
	        admin.disableTable(tableName);
	
	        // Delete an existing column family
	        admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));
	
	        // Delete a table (Need to be disabled first)
	        admin.deleteTable(tableName);
	        admin.close();
 		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static void allMethodSchema (Configuration config){
        Connection connection;
		try {
			connection = ConnectionFactory.createConnection(config);
		
             Admin admin = connection.getAdmin();

          TableName tableName = TableName.valueOf(TABLE_NAME);
          if (!admin.tableExists(tableName)) {
            System.out.println("Table does not exist.");
            System.exit(-1);
          }

          HTableDescriptor table = new HTableDescriptor(tableName);

          System.out.print("ALL-add NewCF. ");
          // Update existing table
          HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
          newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
          admin.addColumn(tableName, newColumn);
          System.out.print("ALL-add done. ");

          System.out.print("ALL-modify OldCF. ");
          // Update existing column family
          HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
          existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
          table.modifyFamily(existingColumn);
          admin.modifyTable(tableName, table);
          System.out.print("ALL-modify done. ");

          System.out.print("ALL-disable table. ");
          // Disable an existing table
          admin.disableTable(tableName);
          System.out.print("ALL-disable done. ");

          System.out.print("ALL-delete column. ");
          // Delete an existing column family
          admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));
          System.out.print("ALL-delete column done. ");

          System.out.print("ALL-delete table. ");
          // Delete a table (Need to be disabled first)
          admin.deleteTable(tableName);
          System.out.print("ALL-delete table done. ");
          admin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
      }
    
    
}
