package com.acxiom.ppm.incrementaldataloadvalidation;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//Comment added 




public class ValidatorReducer extends Reducer< Text, Text, Text, Text> {
	private String sourceHeaderFile ;
	private String targetHeaderFile ;
	private String targetHiveTableName;
	private static final Map<String, String> headerLocationWithTableNames = new HashMap<String, String>();

	Properties targetDWConfiguration=null;
	Properties sourceDWConfiguration=null;
	static Properties properties=null;
	String currentTablename=null;

	Text KeyOut = new Text();
	Text ValueOut = new Text();
	@Override
	protected void setup(Context context){

		Configuration conf = context.getConfiguration();

		sourceHeaderFile = conf.get("sourcetable.header.configurationfile.path");
		targetHeaderFile = conf.get("targettable.header.configurationfile.path");

		targetDWConfiguration = new Properties();
		sourceDWConfiguration = new Properties();		
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);


			if (sourceHeaderFile != null) {			
				sourceDWConfiguration = ValidatorMapper.loadProperties(sourceHeaderFile, fs);
				for (Map.Entry<Object, Object> e : sourceDWConfiguration.entrySet()) {
					String key = (String) e.getKey();
					String value = (String) e.getValue();				  
					headerLocationWithTableNames.put(key, value);					 
				}
			}

			if (targetHeaderFile != null) {			
				targetDWConfiguration = ValidatorMapper.loadProperties(targetHeaderFile, fs);
				for (Map.Entry<Object, Object> e : targetDWConfiguration.entrySet()) {
					targetHiveTableName = (String) e.getKey();
					String key = (String) e.getKey();
					String value = (String) e.getValue();				  
					headerLocationWithTableNames.put(key, value);					 
				}
			}		


		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}  


	}

	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context){
	
		Map<String, String> targetHiveTable = new HashMap<String, String>();
		Map<String, String> soruceHiveTable = new HashMap<String, String>();
          for(Text tableNameWithData:values){
        	  
        	  String[] tableDataSeperator = tableNameWithData.toString().split(":",2);
        	  if(tableDataSeperator[0].equals(targetHiveTableName)){
        		  //HiveDataTableValueSet
        		  String HiveDataTableHeaderValue = headerLocationWithTableNames.get(tableDataSeperator[0]);
        		  if(!HiveDataTableHeaderValue.isEmpty()){
        			  
        		  }
        				  
        	  }
        	 
          }

	}


	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
	
      }
}
