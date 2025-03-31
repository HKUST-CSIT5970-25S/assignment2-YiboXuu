package hk.ust.csit5970;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Compute the bigram count using "pairs" approach
 */
public class BigramFrequencyPairs extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BigramFrequencyPairs.class);

	/*
	 * TODO: write your Mapper here.
	 */
	private static class MyMapper extends
			Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

		// Reuse objects to save overhead of object creation.
		private static final IntWritable ONE = new IntWritable(1);
		private static final PairOfStrings BIGRAM = new PairOfStrings();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = ((Text) value).toString();
			String[] words = line.trim().split("\\s+");
			
		
			if (words.length < 2) return;  
            
            for (int i = 0; i < words.length - 1; i++) {  
                // 对于每个二元组 (a,b)，我们需要发出两种类型的键值对：  
                // 1. 二元组本身，值为1: ((a,b), 1)  
                // 2. 左词加特殊标记，用于计算总频次: ((a,*), 1)  
                
                if (words[i].length() == 0 || words[i+1].length() == 0) continue;  
                
                // 发出二元组 (a,b)  
                BIGRAM.set(words[i], words[i+1]);  
                context.write(BIGRAM, ONE);  
                
                // 发出左词的总频次计数 (a,*)  
                BIGRAM.set(words[i], "");  
                context.write(BIGRAM, ONE);  
            }  
		}
	}

	/*
	 * TODO: Write your reducer here.
	 */
	private static class MyReducer extends
			Reducer<PairOfStrings, IntWritable, PairOfStrings, FloatWritable> {

		// Reuse objects.
		private final static FloatWritable VALUE = new FloatWritable();
		private int marginal = 0; 

		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */

			// 检查是否是左侧词的总频次计数  
            if (key.getRightElement().equals("")) {  
                // 累加该左侧词的总出现次数  
                marginal = 0;  
                for (IntWritable value : values) {  
                    marginal += value.get();  
                }  
                
                // 输出左侧词的总出现次数  
                VALUE.set((float) marginal);  
                context.write(key, VALUE);  
            } else {  
                // 计算特定二元组的出现次数  
                int count = 0;  
                for (IntWritable value : values) {  
                    count += value.get();  
                }  
                
                // 若左侧词的总频次已知，计算相对频率  
                if (marginal > 0) {  
                    VALUE.set((float) count / marginal);  
                    context.write(key, VALUE);  
                }  
            }  
		}
	}
	
	private static class MyCombiner extends
			Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
		private static final IntWritable SUM = new IntWritable();

		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			// 在 Combiner 中，我们只需要将相同键的计数值相加  
            int sum = 0;  
            for (IntWritable value : values) {  
                sum += value.get();  
            }  
            SUM.set(sum);  
            context.write(key, SUM);  
		}
	}

	/*
	 * Partition bigrams based on their left elements
	 */
	private static class MyPartitioner extends
			Partitioner<PairOfStrings, IntWritable> {
		@Override
		public int getPartition(PairOfStrings key, IntWritable value,
				int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE)
					% numReduceTasks;
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public BigramFrequencyPairs() {
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String NUM_REDUCERS = "numReducers";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of reducers").create(NUM_REDUCERS));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			return -1;
		}

		// Lack of arguments
		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		String outputPath = cmdline.getOptionValue(OUTPUT);
		int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
				.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

		LOG.info("Tool: " + BigramFrequencyPairs.class.getSimpleName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - number of reducers: " + reduceTasks);

		// Create and configure a MapReduce job
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName(BigramFrequencyPairs.class.getSimpleName());
		job.setJarByClass(BigramFrequencyPairs.class);

		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(FloatWritable.class);

		/*
		 * A MapReduce program consists of three components: a mapper, a
		 * reducer, a combiner (which reduces the amount of shuffle data), and a partitioner
		 */
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already.
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		// Time the program
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BigramFrequencyPairs(), args);
	}
}

