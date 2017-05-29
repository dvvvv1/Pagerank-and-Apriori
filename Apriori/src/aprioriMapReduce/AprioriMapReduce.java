package aprioriMapReduce;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AprioriMapReduce extends Configured implements Tool {

	// globe variables
	private static int totItemNum;
	private static HashMap<Integer, Set> itemTable;
	private static ArrayList<Set<Integer>> C;
	private static Double[] C_sup;
	private static ArrayList<Set<Integer>> L;
	private static Double[] L_sup;
	private static double s_value = 0.045;
	private static int interation_K;
	private static int[] L_size;
	private static boolean indexIniCheck;
	private static long startTime;
	private static long endTime;
	private static long totTime;

	public static void main(String[] args) throws Exception {
		startTime = System.currentTimeMillis();
		if(args.length == 2)
		{
			System.out.println(Arrays.toString(args));
			int res = ToolRunner.run(new Configuration(), new AprioriMapReduce(), args);
					
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
		ArrayList<String> dir = new ArrayList<String>();	
		
		// Setup globe variables
		totItemNum = 0;
		itemTable = new HashMap<Integer, Set>();
		indexIniCheck = false;
		
		// create job control
		JobControl con = new JobControl("AprioriMapReduce");

		// Create map-reduce job for each node
		ControlledJob ItemJob = new ControlledJob(createItem(args[0],FileDirName), null);

		// ini setup before interation
		initialzation();

		interation_K = 0;
		int interation_limit = 4;

		// iterate K times to find possible item-sets.
		// && interation_K < interation_limit
		while(L.size() > 0 )
		{
			createItemSet(L.size());
			interation_K++;
			System.out.println(interation_K);
			filter();
			if(L.size() <= 0)
			{
				break;
			}
			int currentK = interation_K + 1;
			String dirName = "output/K="+ currentK + " S="+s_value+" itemSet_List.txt";
			outputResult(dirName);
		}
		
		int totIntera = interation_K + 1;
		System.out.println(totIntera + " Interations Complete");
		// record the time
		endTime = System.currentTimeMillis();
		totTime = (endTime - startTime)/1000;
		System.out.println("Total time spent: " + totTime + " secs");
		
		// generate result overview report
		report();
		
		return 0;	
	}

	/*
	 * This function is used to create items map-reduce job
	 */
	private Job createItem(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job();
		job.setJarByClass(AprioriMapReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ItemMap.class);
		job.setReducerClass(ItemReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));    
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		return job;
	}

	/*
	 * Initialization function 
	 * This function is used to initialize the C and L tables
	 */
	public static void initialzation() throws IOException {
		C = new ArrayList<Set<Integer>>();
		L = new ArrayList<Set<Integer>>();
		C_sup = new Double[itemTable.size()];
		L_size = new int[30];

		Map<Integer, Set> KeyMap = new TreeMap<Integer, Set>(itemTable);
		
		int C_sup_index = 0;
		for(int tmpK:KeyMap.keySet())
		{
			double tmp_item_sum = (double) KeyMap.get(tmpK).size();
			double tmp_sup = tmp_item_sum / (double) totItemNum;
			Set tmp_key_sup = new TreeSet<Integer>();
			tmp_key_sup.add(tmpK);
		
			C_sup[C_sup_index] = tmp_sup; 
			C_sup_index++;
			
			C.add(tmp_key_sup);
		}

		for(int i = 0; i < C.size(); i++)
		{
			if(C_sup[i] > s_value)
			{
				Set<Integer> tmp = C.get(i);
				L.add(tmp);
			}
		}
		
		// record
		L_size[0] = L.size();

		L_sup = new Double[L.size()];
		int tmpIndex = 0;
		for(int i = 0; i < C.size(); i++)
		{
			if(C_sup[i] > s_value)
			{
				L_sup[tmpIndex] = C_sup[i];
				tmpIndex++;
			}
		}
		
		String dir1 = "output/K=1 S=" + s_value +" ItemSet_List.txt";
		outputResult(dir1);
	}
	/*
	 * This function is the map function which used to read items and boxes.
	 * Output: Key = item id, Value = box id
	 */
	public static class ItemMap extends Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Text box = new Text(String.valueOf(totItemNum));
			for (String token: value.toString().split("\\s+")) {
				Text item = new Text(token);
				context.write(item, box);
			}
			totItemNum++;
		}
	}
	/*
	 * This is the reduce function to calculate the boxes that each item appeared 
	 * output: Key = item Id, Values = {box1, box2, box3 ... box N}
	 */
	public static class ItemReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String output = "";
			// store to Set
			Set<Integer> tmpSet = new TreeSet<Integer>();
			int itemId = Integer.parseInt(key.toString());

			for (Text val : values) {
				int boxId = Integer.parseInt(val.toString());
				tmpSet.add(boxId);
				output = output + "," + val.toString();
			}
			Text boxList = new Text(output.substring(1));

			itemTable.put(itemId, tmpSet);
			context.write(key,boxList);
		}
	}

	/*
	 * This function will calculate support values for each items
	 */
	public static void calculateItemset() {
		C_sup = new Double[C.size()];

		for(int i = 0; i < C.size(); i++)
		{
			Set<Integer> TmpSetForSameBox = new TreeSet<Integer>();
			ArrayList<Integer> tmpList = new ArrayList<Integer>(C.get(i));
			if(tmpList.size() > 1)
			{
				String tmpKey = String.valueOf(tmpList.get(0));
				double tmpkey2 = Double.parseDouble(tmpKey);
				int key = (int) tmpkey2;
				Set<Integer> copy = new TreeSet<Integer>();
				copy.addAll(itemTable.get(key));
				TmpSetForSameBox.addAll(copy);
				for(int j = 1; j < tmpList.size(); j++)
				{
					String innerKey1 = String.valueOf(tmpList.get(j));
					double innerKey2 = Double.parseDouble(innerKey1);
					int innerKey3 = (int) innerKey2;
					TmpSetForSameBox.retainAll(itemTable.get(innerKey3));
				}
			}

			C_sup[i] = (double)TmpSetForSameBox.size() / (double) totItemNum;
		}
	}

	/*
	 * This function will create new item-set
	 */
	public static void createItemSet(int size) {

		int i = 0;
		int j = 0;
		C = new ArrayList<Set<Integer>>();

		for (i = 0; i < size - 1; i++)
		{
			for (j = i + 1; j < size; j++)
			{
				//System.out.println(i+","+j);
				Set<Integer> tmp = new TreeSet<Integer>();
				tmp.addAll(L.get(i));
				tmp.addAll(L.get(j));
				int checkDegree = interation_K + 2;
				if(tmp.size() > checkDegree && checkDegree >= 2)
				{
					continue;
				}
				//System.out.println(tmp);
				if(!C.contains(tmp))
				{
					C.add(tmp);
				}
			}
		}

		// calculate new support value for new itemset
		calculateItemset();
	}

	/*
	 * This function will filter all support values
	 */
	public static void filter() {
		L = new ArrayList<Set<Integer>>();

		for(int i = 0; i < C.size(); i++)
		{
			if(C_sup[i] > s_value)
			{
				Set<Integer> tmp = C.get(i);
				L.add(tmp);
			}
		}
		
		L_size[interation_K] = L.size();
		L_sup = new Double[L.size()];
		int tmpIndex = 0;
		for(int i = 0; i < C.size(); i++)
		{
			if(C_sup[i] > s_value)
			{
				L_sup[tmpIndex] = C_sup[i];
				System.out.println(L.get(tmpIndex) + ": " + L_sup[tmpIndex]);
				tmpIndex++;
			}
		}
	}

	/*
	 * This function is used to output result for after each iteration
	 */
	public static void outputResult(String outputPath) throws IOException {
		// output Item set tables
		File newTextFile = new File(outputPath);
		FileWriter CurrentResult = new FileWriter(newTextFile);
		for (int i = 0; i < L.size(); i++) 
		{
			CurrentResult.write(L.get(i) + ": " + L_sup[i] + "\n");
		}
		CurrentResult.close();
		
		int current_K = interation_K + 1;
		//output found report
		String tmpDir = outputPath.substring(0, outputPath.length()-16) + " Found Report.txt";
		File newTextFile2 = new File(tmpDir);
		FileWriter CurrentResult2 = new FileWriter(newTextFile2);
		CurrentResult2.write("Found " + L_size[interation_K] + " frequent itemsets of size(K) " + current_K + " (with support " + s_value+")" + "\n");
		int totFound = 0;
		for(int i = 0; i < current_K; i++)
		{
			totFound = totFound + L_size[i];
		}
		CurrentResult2.write("Total found " + totFound + " frequent itemsets for K = " + current_K + " (with support " + s_value+")" + "\n");
		CurrentResult2.close();
	}
	
	/*
	 * This function is used to output final report for the result
	 */
	public static void report() throws IOException
	{
		int current_K = interation_K + 1;
		String tmpDir = "output/Final Result Report.txt";
		File newText = new File(tmpDir);
		FileWriter FinalResult = new FileWriter(newText);
		FinalResult.write(current_K + " Interations completed" + "\n");
		int totFound = 0;
		for(int i = 0; i < current_K; i++)
		{
			totFound = totFound + L_size[i];
		}
		FinalResult.write("Total found " + totFound + " frequent itemsets for K = " + current_K + " (with support " + s_value+")" + "\n");
		FinalResult.write("Total time spent: " + totTime + " secs" + "\n");

		FinalResult.close();
	}
}
