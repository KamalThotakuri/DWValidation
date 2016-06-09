package com.acxiom.ppm.incrementaldataloadvalidation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//Comment added 




public class ValidatorReducer extends Reducer< Text, Text, Text, Text> {
	private String sourceHeaderFile ;
	private String targetHeaderFile ;
	private String targetHiveTableName;
	private static final Map<String, String[]> headerLocationWithTableNames = new HashMap<String, String[]>();

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
					headerLocationWithTableNames.put(key, ValidatorReducer.loadHeader(value, fs));					 
				}
			}

			if (targetHeaderFile != null) {			
				targetDWConfiguration = ValidatorMapper.loadProperties(targetHeaderFile, fs);
				for (Map.Entry<Object, Object> e : targetDWConfiguration.entrySet()) {
					targetHiveTableName = (String) e.getKey();
					String key = (String) e.getKey();
					String value = (String) e.getValue();				  
					headerLocationWithTableNames.put(key, ValidatorReducer.loadHeader(value, fs));					 
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
		Map<String, String> soruceTable = new HashMap<String, String>();
		for(Text tableNameWithData:values){

			String[] tableDataSeperator = tableNameWithData.toString().split(":",2);
			if(tableDataSeperator[0].equals(targetHiveTableName)){
				//HiveDataTableValueSet
				String[] HiveDataTableHeaderValue = headerLocationWithTableNames.get(tableDataSeperator[0]);
				String[] actualDataValue = tableDataSeperator[0].split("\t");
				if(HiveDataTableHeaderValue.length !=0){

					for(int i = 0; i < HiveDataTableHeaderValue.length; i++)
					{
						targetHiveTable.put(HiveDataTableHeaderValue[i], actualDataValue[i]);
					}
				}else {
					String[] sourceTableHeaderValue = headerLocationWithTableNames.get(tableDataSeperator[0]);
					String[] sourceactualDataValue = tableDataSeperator[0].split("\t");
					if(sourceTableHeaderValue.length !=0){

						for(int i = 0; i < sourceTableHeaderValue.length; i++)
						{
							soruceTable.put(sourceTableHeaderValue[i], sourceactualDataValue[i]);
						}
					}

				}
			} 
		}

	}


	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {

	}


	private  static String[] loadHeader(String headerFile, FileSystem fs) {
		String line; // Each record read in form of String
		String[] tokens = null; // each column stored in tokens
		try {	

			FSDataInputStream fis = fs.open(new Path(headerFile));
			// Read variable definition of the variables on which missing imputation needs to be done
			BufferedReader HeaderValues = new BufferedReader(new InputStreamReader(fis));
			while ((line = HeaderValues.readLine()) != null) {

				tokens = line.split("\t"); // Split it by tab

				/*for (int i = 0; i < tokens.length; i++) {
						tokens[i]=tokens[i].toLowerCase();

					}*/
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return tokens;
	}

}
