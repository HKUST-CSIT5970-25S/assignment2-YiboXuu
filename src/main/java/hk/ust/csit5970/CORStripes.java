package hk.ust.csit5970;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Compute the bigram count using "pairs" approach
 */
public class CORStripes extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CORStripes.class);

	/*
	 * TODO: write your first-pass Mapper here.

	 */
	private static class CORMapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable ONE = new IntWritable(1);  
        private final static Text WORD = new Text();  
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> word_set = new HashMap<String, Integer>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String clean_doc = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizer = new StringTokenizer(clean_doc);
			/*
			 * TODO: Your implementation goes here.
			 */
			while (doc_tokenizer.hasMoreTokens()) {  
                String token = doc_tokenizer.nextToken();  
                if (token.length() > 0) {  
                    WORD.set(token);  
                    context.write(WORD, ONE);  
                }  
            }  
		}
	}

	/*
	 * TODO: Write your first-pass reducer here.
	 */
	private static class CORReducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private final static IntWritable TOTAL = new IntWritable();  
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			int sum = 0;  
            for (IntWritable value : values) {  
                sum += value.get();  
            }  
            TOTAL.set(sum);  
            context.write(key, TOTAL);  
		}
	}

/*
	 * TODO: Write your second-pass Mapper here.
	 */
	public static class CORStripesMapper2 extends Mapper<LongWritable, Text, Text, MapWritable> {
		
		
		private final static Text WORD = new Text();  
        private final static MapWritable STRIPES = new MapWritable();  
        private final static Text NEXT_WORD = new Text();  
        private final static IntWritable ONE = new IntWritable(1);  
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Set<String> sorted_word_set = new TreeSet<String>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String doc_clean = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizers = new StringTokenizer(doc_clean);
			 // 收集文档中的所有不同单词  
			while (doc_tokenizers.hasMoreTokens()) {  
				String token = doc_tokenizers.nextToken();  
				if (token.length() > 0) {  
					sorted_word_set.add(token);  
				}  
			}  
			
			// 处理这一行的所有单词对 - 每个唯一的单词对只输出一次  
			List<String> wordList = new ArrayList<String>(sorted_word_set);  
			MapWritable stripes = new MapWritable();  
			
			for (int i = 0; i < wordList.size(); i++) {  
				for (int j = i + 1; j < wordList.size(); j++) {  
					// 确保单词 A 字母顺序小于 B  
					String wordA = wordList.get(i);  
					String wordB = wordList.get(j);  
					
					if (wordA.compareTo(wordB) < 0) {  
						WORD.set(wordA);  
						NEXT_WORD.set(wordB);  
					} else {  
						WORD.set(wordB);  
						NEXT_WORD.set(wordA);  
					}  
					
					stripes.clear();  
					stripes.put(NEXT_WORD, ONE);  
					context.write(WORD, stripes);  
				}  
			}  
		}  
		
	
	}

	/*
	 * TODO: Write your second-pass Combiner here.
	 */
	public static class CORStripesCombiner2 extends Reducer<Text, MapWritable, Text, MapWritable> {
		static IntWritable ZERO = new IntWritable(0);
		private final static MapWritable COMBINED_STRIPES = new MapWritable();  
        private final static IntWritable COUNT = new IntWritable();  

		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			COMBINED_STRIPES.clear();  
            for (MapWritable stripe : values) {  
                for (Writable k : stripe.keySet()) {  
                    Text nextWord = (Text)k;  
                    IntWritable count = (IntWritable)stripe.get(nextWord);  
                    
                    if (COMBINED_STRIPES.containsKey(nextWord)) {  
                        IntWritable existingCount = (IntWritable)COMBINED_STRIPES.get(nextWord);  
                        COUNT.set(existingCount.get() + count.get());  
                        COMBINED_STRIPES.put(nextWord, COUNT);  
                    } else {  
                        COMBINED_STRIPES.put(nextWord, count);  
                    }  
                }  
            }  
            context.write(key, COMBINED_STRIPES);  
        
		}
	}

	/*
	 * TODO: Write your second-pass Reducer here.
	 */
	public static class CORStripesReducer2 extends Reducer<Text, MapWritable, PairOfStrings, DoubleWritable> {
		private static Map<String, Integer> word_total_map = new HashMap<String, Integer>();
		private static IntWritable ZERO = new IntWritable(0);
		private final static PairOfStrings BIGRAM = new PairOfStrings();  
        private final static DoubleWritable RELATIVE_FREQ = new DoubleWritable();  


		/*
		 * Preload the middle result file.
		 * In the middle result file, each line contains a word and its frequency Freq(A), seperated by "\t"
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path middle_result_path = new Path("mid/part-r-00000");
			Configuration middle_conf = new Configuration();
			try {
				FileSystem fs = FileSystem.get(URI.create(middle_result_path.toString()), middle_conf);

				if (!fs.exists(middle_result_path)) {
					throw new IOException(middle_result_path.toString() + "not exist!");
				}

				FSDataInputStream in = fs.open(middle_result_path);
				InputStreamReader inStream = new InputStreamReader(in);
				BufferedReader reader = new BufferedReader(inStream);

				LOG.info("reading...");
				String line = reader.readLine();
				String[] line_terms;
				while (line != null) {
					line_terms = line.split("\t");
					word_total_map.put(line_terms[0], Integer.valueOf(line_terms[1]));
					LOG.info("read one line!");
					line = reader.readLine();
				}
				reader.close();
				LOG.info("finished！");
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}

		/*
		 * TODO: Write your second-pass Reducer here.
		 */
		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			   // 合并所有stripe  
			MapWritable combinedStripe = new MapWritable();  
			
			for (MapWritable stripe : values) {  
				for (Writable k : stripe.keySet()) {  
					Text nextWord = (Text)k;  
					IntWritable count = (IntWritable)stripe.get(nextWord);  
					
					if (combinedStripe.containsKey(nextWord)) {  
						IntWritable existingCount = (IntWritable)combinedStripe.get(nextWord);  
						IntWritable newCount = new IntWritable(existingCount.get() + count.get());  
						combinedStripe.put(nextWord, newCount);  
					} else {  
						combinedStripe.put(nextWord, count);  
					}  
				}  
			}  

			// 获取左侧词的总频率  
			String leftWord = key.toString();  
			if (word_total_map.containsKey(leftWord)) {  
				int leftWordTotal = word_total_map.get(leftWord);  

				// 计算并输出每个二元组的相关系数  
				for (Writable k : combinedStripe.keySet()) {  
					Text nextWordText = (Text)k;  
					String rightWord = nextWordText.toString();  
					
					// 确保 A 字母顺序小于 B  
					if (leftWord.compareTo(rightWord) < 0) {  
						BIGRAM.set(leftWord, rightWord);  
					} else {  
						BIGRAM.set(rightWord, leftWord);  
					}  
					
					IntWritable countWritable = (IntWritable)combinedStripe.get(nextWordText);  
					int freqAB = countWritable.get();  
					
					// 获取右侧词的总频率  
					if (word_total_map.containsKey(rightWord)) {  
						int rightWordTotal = word_total_map.get(rightWord);  
						
						// 计算相关系数：COR(A, B) = Freq(A,B) / (Freq(A) * Freq(B))  
						double correlation = (double) freqAB / (leftWordTotal * rightWordTotal);  
						
						RELATIVE_FREQ.set(correlation);  
						context.write(BIGRAM, RELATIVE_FREQ);  
					}  
				} 
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public CORStripes() {
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
		String middlePath = "mid";
		String outputPath = cmdline.getOptionValue(OUTPUT);

		int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
				.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

		LOG.info("Tool: " + CORStripes.class.getSimpleName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - middle path: " + middlePath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - number of reducers: " + reduceTasks);

		// Setup for the first-pass MapReduce
		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1, "Firstpass");

		job1.setJarByClass(CORStripes.class);
		job1.setMapperClass(CORMapper1.class);
		job1.setReducerClass(CORReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(middlePath));

		// Delete the output directory if it exists already.
		Path middleDir = new Path(middlePath);
		FileSystem.get(conf1).delete(middleDir, true);

		// Time the program
		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		// Setup for the second-pass MapReduce

		// Delete the output directory if it exists already.
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf1).delete(outputDir, true);


		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Secondpass");

		job2.setJarByClass(CORStripes.class);
		job2.setMapperClass(CORStripesMapper2.class);
		job2.setCombinerClass(CORStripesCombiner2.class);
		job2.setReducerClass(CORStripesReducer2.class);

		job2.setOutputKeyClass(PairOfStrings.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MapWritable.class);
		job2.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));

		// Time the program
		startTime = System.currentTimeMillis();
		job2.waitForCompletion(true);
		LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CORStripes(), args);
	}
}
