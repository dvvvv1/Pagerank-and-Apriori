package pageRank;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank extends Configured implements Tool {

	private static int nums; // total number of pages
	private static float beta = 0.80f; // Variable 
	private static float[] v_tmp;
	private static float[] v_result;
	private static boolean flag_firstAttemp;
	private static boolean flag_copied;
	private static BidiMap mask;
	private static int mask_Index;

	public static void main(String[] args) throws Exception {
		if(args.length == 2)
		{
			System.out.println(Arrays.toString(args));
			int res = ToolRunner.run(new Configuration(), new PageRank(), args);

			System.exit(res);
		}
		else
		{
			System.exit(-1);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		// File path setup
		String FileDir = "output";
		String FileDirName = FileDir + "/part-r-0000";
		String FileDirName2 = FileDir + "/part-r-0001";
		String FileDirName3 = FileDir + "/part-r-0002";
		String FileDirInterate1 = FileDir + "/part-r-000";
		String TopTenResultDir = FileDir + "/SortByRank";

		// mask of node ID
		mask = new DualHashBidiMap();
		mask_Index = 0;

		// create job control
		JobControl con = new JobControl("PageRank");

		// Create mask for each node
		ControlledJob MaskJob = new ControlledJob(createMask(args[0],FileDirName), null);
		// set mask up for each node id
		ControlledJob SetMaskJob = new ControlledJob(createMaskData(FileDirName,FileDirName2), null);
		// set globe variables
		setGlobe();
		// Create Matrix M
		ControlledJob MatrixJob = new ControlledJob(createMatrix(FileDirName2,FileDirName3), null);
		// page rank iterations
		int tmpIndex;
		String ResultDir = "";
		for(int i = 0; i < 30; i++)
		{
			tmpIndex = i + 4;
			String tmpFileDir = FileDirInterate1 + tmpIndex;
			ResultDir = tmpFileDir;
			ControlledJob PageRankJob1 = new ControlledJob(calPageRank(FileDirName3, tmpFileDir), null);
		}
		// find top ten pageRank
		ControlledJob FindMaxJob = new ControlledJob(findMax(ResultDir, TopTenResultDir), null);

		// output top ten pageRank node
		String inputPathName = TopTenResultDir+"/part-r-00000";
		String outputPathName = FileDir + "/TopTen.txt";
		readTopTen(inputPathName,outputPathName);

		//con.addJob(MatrixJob);
		//con.addJob(rowJob);
		//con.addJob(pageRankJob1);
		return 0;	
	}

	/*
	 * Create Mask to each discontinuous node
	 */
	private Job createMask(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job();
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MaskMap.class);
		job.setReducerClass(MaskReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));    
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		return job;
	}

	/*
	 * This function will set generated mask number to each node id.
	 */
	private Job createMaskData(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job();
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(setMaskMap.class);
		job.setReducerClass(setMaskReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));    
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		return job;
	}

	/*
	 * Create Matrix M based on column
	 */
	private Job createMatrix(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job();
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MatrixMap.class);
		job.setReducerClass(MatrixReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));    
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		return job;
	}

	/*
	 * Calculate pageRank for each masked node
	 */
	private Job calPageRank(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job();
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PageRankMap.class);
		job.setReducerClass(PageRankReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));    
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		return job;
	}

	/*
	 * Find the top ten pageRank nodes
	 */
	private Job findMax(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job();
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(findMaxMap.class);
		job.setReducerClass(findMaxReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));    
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		return job;
	}

	/*
	 * This function will set total number of nodes and initial V
	 */
	public static void setGlobe()
	{
		nums = mask_Index;
		v_result = new float[nums];
		v_tmp = new float[nums];
		flag_firstAttemp = true;
		for(int j = 0; j < nums; j++)
		{
			v_result[j] = 1.0f / nums;
			v_tmp[j] = 1.0f / nums;
		}
	}

	/*
	 * This function will read final result and store top 10 pageRank
	 * nodes into a separate file.
	 */
	public static void readTopTen(String readPath, String outputPath) throws IOException
	{
		FileInputStream in = new FileInputStream(readPath);
		int line_limit = 10;
		int index = 0;
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		File newTextFile = new File(outputPath);
		FileWriter topTen = new FileWriter(newTextFile);
		HashMap<Integer, Float> rankedMap = new HashMap<Integer, Float>();
		Map<Integer, Float> topTenMap = new java.util.HashMap<Integer, Float>();
		

		String tmp = "";
		while((tmp = br.readLine()) != null && index < line_limit)
		{
			String[] tmp1 = tmp.split("\\s+");
			int node = Integer.parseInt(tmp1[0]);
			float rank = Float.parseFloat(tmp1[1]);
			
			rankedMap.put(node, rank);
		}
		
		topTenMap = sortByValue(rankedMap);
		for (Integer str : topTenMap.keySet()) {
			if(index >= 10)
			{
				break;
			}
			float tmpRank = topTenMap.get(str);
		    topTen.write(str+" "+ tmpRank + "\n");
		    index++;
		}

		topTen.close();
	}
	
	/*
	 * This function is adopted from overflow
	 * Ref:http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java
	 * And it is used to sort the HashMap by its key's values.
	 */
	private static <K, V> Map<K, V> sortByValue(Map<K, V> map) {
	    List<Map.Entry<K, V>> InputL = new LinkedList<>(map.entrySet());
	    Collections.sort(InputL, new Comparator<Object>() {
	        @SuppressWarnings("unchecked")
	        public int compare(Object obj2, Object obj1) {
	        	//compare the values and order the result by ascend order
	            return ((Comparable<V>) ((Map.Entry<K, V>) (obj1)).getValue()).compareTo(((Map.Entry<K, V>) (obj2)).getValue());
	        }
	    });
	    //setup result container
	    Map<K, V> output = new LinkedHashMap<>();
	    for (Iterator<Map.Entry<K, V>> it = InputL.iterator(); it.hasNext();) {
	        Map.Entry<K, V> TmpEn = (Map.Entry<K, V>) it.next();
	        output.put(TmpEn.getKey(), TmpEn.getValue());
	    }
	    return output;
	}

	/*
	 * The following two functions are map-reduce functions.
	 * These two functions will read initial data and generate mask index for
	 * each node id.
	 */
	public static class MaskMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] token = value.toString().split("\\s+");
			if(!token[0].equals("#"))
			{
				Text from = new Text(token[0]);
				Text to = new Text(token[1]);
				context.write(from, to);
			}
		}
	}

	public static class MaskReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int tmpIndex = Integer.parseInt(key.toString());
			mask.put(tmpIndex, mask_Index);
			mask_Index++;
			for (Text val : values) {
				context.write(key, val);
			}
		}
	}

	/*
	 * The following two functions are map-reduce functions.
	 * These two functions will read sorted data and set generated mask id to
	 * each node.
	 */
	public static class setMaskMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] token = value.toString().split("\\s+");

			int tmpVal = Integer.parseInt(token[1]);
			if(!mask.containsKey(tmpVal))
			{
				mask.put(tmpVal, mask_Index);
				mask_Index++;
			}

			Text from = new Text(token[0]);
			Text to = new Text(token[1]);
			context.write(from, to);
		}
	}

	public static class setMaskReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// set masked key
			int tmpKey = Integer.parseInt(key.toString());
			int outputKey = (int) mask.get(tmpKey);
			Text outKey = new Text(String.valueOf(outputKey));

			for (Text val : values) {
				// set masked value
				int tmpVal = Integer.parseInt(val.toString());
				//System.out.println(val.toString());
				int outputVal = (int) mask.get(tmpVal);
				Text outVal = new Text(String.valueOf(outputVal));
				context.write(outKey, outVal);
			}
		}
	}

	public static class MatrixMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] token = value.toString().split("\\s+");
			Text from = new Text(token[0]);
			Text to = new Text(token[1]);
			context.write(from, to);
		}
	}

	public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// out link sum
			float sum = 0;
			ArrayList<String> toNodeList = new ArrayList<String>();
			for (Text val : values) {
				toNodeList.add(val.toString());
				sum++;
			}

			for (int i = 0; i < toNodeList.size(); i++)
			{
				Text from = key;
				Text to = new Text(toNodeList.get(i)+" "+sum);
				context.write(to, from);
			}
		}
	}

	public static class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// token[0] is ToNode
			// token[1] is degree
			// token[2] is FromNode
			String[] token = value.toString().split("\\s+");

			if(flag_copied == false)
			{
				//copy previous result to current v
				for(int i = 0; i < nums; i++)
				{
					v_result[i] = v_tmp[i];
				}
				flag_copied = true;
			}

			Text k = new Text(token[0]);
			float tmpDegree = Float.parseFloat(token[1]);
			int tmpToNodeIndex = Integer.parseInt(token[2]);
			float Mv = 1.0f / tmpDegree * v_result[tmpToNodeIndex];
			Text MvOut = new Text(String.valueOf(Mv));

			context.write(k, MvOut);  
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// setup flag
			flag_copied = false;
			float VMsum = 0.0f;

			for (Text val : values) {
				float tmpMVn = Float.parseFloat(val.toString());
				VMsum = VMsum + tmpMVn;
			}

			float resultV = beta * VMsum + (1-beta)/nums;

			//store result
			int index = Integer.parseInt(key.toString());
			v_tmp[index] = resultV;
			Text vv = new Text(String.valueOf(resultV));

			context.write(key, vv); 
		}
	}

	public static class findMaxMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] token = value.toString().split("\\s+");
			Text node = new Text(token[0]);
			Text rankV = new Text(token[1]);
			context.write(node, rankV);
		}
	}

	public static class findMaxReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {	
				//transfer the masked index to original node id and output
				int tmpKey1 = Integer.parseInt(key.toString());
				int tmpKey2 = (int) mask.getKey(tmpKey1);
				Text outputKey = new Text(String.valueOf(tmpKey2));
				context.write(outputKey, val); 
			}  
		}
	}

}
