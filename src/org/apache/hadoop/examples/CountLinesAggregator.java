package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is an example Hadoop Map/Reduce application.
 * It takes in several outputs and adds the values by key
 *
 * To run: bin/hadoop jar build/countlinesaggregator.jar 
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dirs</i> <i>out-dir</i>
 * e.g.
 *  bin/hadoop jar countlinesaggregator.jar /gutenberg-output1 /gutenberg-output2 /final-output
 */
public class CountLinesAggregator extends Configured implements Tool {
	/**
	 * Aggregate keys and values.
	 * For each line of input, break the line into words and emit them as
	 * (<b>lines</b>, <b>val</b>).
	 */
	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line, "\n");
			String token = itr.nextToken();
			if(token.length() >0 ) {
				String[] splits = token.split("#SEP#");
				if(splits[0] != null && splits[1] != null &&
						splits[0].length() > 0 && splits[1].length() > 0) {
					String k = splits[0];
					String v = splits[1];
					word.set(k.trim());
					IntWritable val = new IntWritable(Integer.valueOf(v));
					output.collect(word, val);
				}
			}
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values as keys.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<Text, IntWritable, IntWritable, Text> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			//String qqq="";
			while (values.hasNext()) {
				int v=values.next().get();
				//qqq+=v+",";
				sum += v;
			}
			
			//System.out.println(key + ": " + qqq);
			output.collect(new IntWritable(sum), key);
		}
	}


	static int printUsage() {
		System.out.println("countlinesaggregator [-m <maps>] [-r <reduces>] <input> <output>");
		System.out.println("countlinesaggregator [-m <maps>] [-r <reduces>] <input1,input2,etc...> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for word count map/reduce program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the
	 *                     jyob tracker.
	 */
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CountLinesAggregator.class);
		conf.setJobName("countlinesaggregator");

		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		//conf.setNumReduceTasks(1);
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() < 2) {
			System.out.println("ERROR: Wrong number of parameters: " + other_args.size());
			return printUsage();
		}

		String inputpath = "";
		for(int i=0; i<other_args.size()-1; i++) {
			if(i<other_args.size()-2)
				inputpath += other_args.get(i) + ",";
			else
				inputpath += other_args.get(i);
		}
		String outputpath=other_args.get(other_args.size()-1);

		System.out.println("Input path: " + inputpath);
		System.out.println("Output path: " + outputpath);
		FileInputFormat.setInputPaths(conf, inputpath);
		FileOutputFormat.setOutputPath(conf, new Path(outputpath));		

		JobClient.runJob(conf);
		return 0;
	}


	/**
	 * 		
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * 
	 * To run: bin/hadoop jar countlinesaggregator.jar <i>in-dir1,in-dir2,in-dir3,etc... /aggregate-output</i>
	 * <i>out-dir</i> <i>numOfReducers</i> textinputformat
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CountLinesAggregator(), args);
		System.exit(res);
	}
}


