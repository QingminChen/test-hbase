package com.qingmin.test.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

//import javax.security.auth.login.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 *
 */
public class CRDUApp {
	
	private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
	  private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";
	  private static boolean exists = false;
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
        
        //listSchemaTables(configuration);
        //addRecords(configuration);
//        getRecords(configuration);
//        exists = isExists(configuration);
//        System.out.println("exists " + exists);
//     
//		scanTable(configuration);
        queryByFilter(configuration);
        //deleteDatas(configuration);
        
    }
    
    /**
     * 安装条件检索数据
     * @param connection
     */
    private static void queryByFilter(Configuration config) {
        // 简单分页过滤器示例程序
        Filter filter = new PageFilter(15);     // 每页15条数据
        int totalRows = 0;
        byte [] lastRow = null;
        
        //RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator("\\d*[:]201303\\d*$"));  也有这种过滤器
        
        Scan scan = new Scan();
        scan.setFilter(filter);
        
        /**
         * scan.setStartRow(Bytes.toBytes("15002159264"));
         * scan.setStopRow(Bytes.toBytes("18002159264"));
         * */
         
        // 略
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Connection connection;
    	try {
			connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(tableName);
			ResultScanner resultScanner = table.getScanner(scan);
//			Iterator<Result> iterator = resultScanner.iterator();
//			
//			while(iterator.hasNext()){
//			   System.out.println(iterator.next()+", ");
//			}
			for(Result rs : resultScanner){
				for(Cell cell : rs.rawCells()){
					System.out.println(cell.toString());
				}
			}
			
			
    	}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
     
    /**
     * 删除数据
     * @param connection
     * @throws IOException 
     */
    private static void deleteDatas(Configuration config){
//    	HRegionInfo hRegionInfo= new HRegionInfo();
//    	HRegionServer hRegionServer = new HRegionServer(config);
//    	HRegion hRegion = new HRegion();
        TableName tableName = TableName.valueOf("qingmin");
        byte [] family = Bytes.toBytes("cf");
        byte [] row = Bytes.toBytes("rk1");
        Delete delete = new Delete(row);
         
        // @deprecated Since hbase-1.0.0. Use {@link #addColumn(byte[], byte[])}
        // delete.deleteColumn(family, qualifier);            // 删除某个列的某个版本
        delete.addColumn(family, Bytes.toBytes("age"),Long.parseLong("1475402599329"));
         
        // @deprecated Since hbase-1.0.0. Use {@link #addColumns(byte[], byte[])}
        // delete.deleteColumns(family, qualifier)            // 删除某个列的所有版本
         
        // @deprecated Since 1.0.0. Use {@link #(byte[])}
        // delete.addFamily(family);                           // 删除某个列族
        Connection connection;
    	try {
			connection = ConnectionFactory.createConnection(config);
	        Table table = connection.getTable(tableName);
	        table.delete(delete);
	        System.out.println("Table " + tableName.getNameAsString() + " delete data succesfully");
    	}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private static boolean isExists (Configuration config){
        /**
         * org.apache.hadoop.hbase.TableName为为代表了表名字的Immutable POJO class对象,
         * 形式为<table namespace>:<table qualifier>。
         *   static TableName  valueOf(byte[] fullName) 
         *  static TableName valueOf(byte[] namespace, byte[] qualifier) 
         *  static TableName valueOf(ByteBuffer namespace, ByteBuffer qualifier) 
         *  static TableName valueOf(String name) 
         *  static TableName valueOf(String namespaceAsString, String qualifierAsString) 
         * HBase系统默认定义了两个缺省的namespace
         *     hbase：系统内建表，包括namespace和meta表
         *     default：用户建表时未指定namespace的表都创建在此
         * 在HBase中，namespace命名空间指对一组表的逻辑分组，类似RDBMS中的database，方便对表在业务上划分。
         * 
        */ 
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Connection connection;
    	try {
			connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
	        boolean exists = admin.tableExists(tableName);
	        if (exists) {
	        	exists = true;
	        	System.out.println("Table " + tableName.getNameAsString() + " already exists.");
	        } else {
	        	exists = false;
	        	System.out.println("Table " + tableName.getNameAsString() + " not exists.");
	        }
	        return exists;
    	}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return exists;
    }
    
    /**
     * 检索数据-表扫描
     * @param connection
     * @throws IOException 
     */
    private static void scanTable(Configuration config){
        TableName tableName = TableName.valueOf(TABLE_NAME);
        byte [] family = Bytes.toBytes(CF_DEFAULT);         
        Scan scan = new Scan();
        scan.addFamily(family);
        Connection connection;
    	try {
			connection = ConnectionFactory.createConnection(config);
	        Table table = connection.getTable(tableName);
	        ResultScanner resultScanner = table.getScanner(scan);
	        for (Iterator<Result> it = resultScanner.iterator(); it.hasNext(); ) {
	            Result result = it.next();
	            List<Cell> cells = result.listCells();
	            for (Cell cell : cells) {
	                String qualifier = new String(CellUtil.cloneQualifier(cell));
	                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
	                System.out.println(qualifier + "\t" + value);
	            }
	        }
    	}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
    
    public static void addRecords(Configuration config){
    	TableName tableName = TableName.valueOf(TABLE_NAME);
    	Connection connection;
    	try {
			connection = ConnectionFactory.createConnection(config);   //HTablePool源码说要用HConnectionManager，如下:@deprecated as of 0.98.1. See {@link HConnection#getTable(String)}.，HConnectionManager源码提示说要用ConnectionFactory@deprecated Please use ConnectionFactory instead，这里的ConnectionFactory就是0.98版本的HTablePool
			Admin admin = connection.getAdmin();
			//HTableDescriptor table= new HTableDescriptor(tableName);
			//Put put = new Put(rowKey.getBytes());
			/**
			   * Add the specified column and value to this Put operation.
			   * @param family family name
			   * @param qualifier column qualifier
			   * @param value column value
			   * @return this
			   * @deprecated Since 1.0.0. Use {@link #addColumn(byte[], byte[], byte[])}
			   */
			//put.add(TABLE_NAME.getBytes(),CF_DEFAULT.getBytes(),value.getBytes());
			//put.addColumn(TABLE_NAME.getBytes(),CF_DEFAULT.getBytes(),value.getBytes());
			//HColumnDescriptor newColumn = new HColumnDescriptor(CF_DEFAULT);
            //newColumn.sesetValue(C_DEFAULT.getBytes(), )
            //admin.addColumn(tableName, newColumn);
			
			
			String [] rows = {"baidu.com", "alibaba.com"};
	        String [] columns = {"owner", "ipstr", "access_server", "reg_date", "exp_date"};
	        String [][] values = {
	            {"Beijing Baidu Technology Co.", "220.181.57.217", "Beijing", "1999-10-11", "2015-10-11"}, 
	            {"Hangzhou Alibaba Advertising Co.", "205.204.101.42", "Hangzhou", "1999=04-15", "2022-05-23"}
	        };
	        
	        byte [] family = Bytes.toBytes(CF_DEFAULT);
	        Table table = connection.getTable(tableName);   //这里的Table就是之前的HTableInterface
	        for (int i = 0; i < rows.length; i++) {
	            System.out.println("========================" + rows[i]);
	            byte [] rowkey = Bytes.toBytes(rows[i]);
	            Put put = new Put(rowkey);
	            for (int j = 0; j < columns.length; j++) {
	                byte [] qualifier = Bytes.toBytes(columns[j]);
	                byte [] value = Bytes.toBytes(values[i][j]);
	                put.addColumn(family, qualifier, value);
	            }
	            table.put(put);
	        }
	        table.close();
	        connection.close();
	    	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static void getRecords(Configuration config){
    	TableName tableName = TableName.valueOf(TABLE_NAME);
    	Connection connection;
    	try {
			connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
	        byte [] family = Bytes.toBytes(CF_DEFAULT);
	        byte [] row = Bytes.toBytes("baidu.com");
	        Table table = connection.getTable(tableName);
	         
	        Get get = new Get(row);
	        get.addFamily(family);
	        // 也可以通过addFamily或addColumn来限定查询的数据
	        Result result = table.get(get);
	        List<Cell> cells = result.listCells();
	        for (Cell cell : cells) {
	            String qualifier = new String(CellUtil.cloneQualifier(cell));
	            String value = new String(CellUtil.cloneValue(cell), "UTF-8");
	            System.out.println(qualifier + "\t" + value);
	        }
    	}catch (IOException e) {
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
	        connection.close();
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
          connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
      }
    
    
}
