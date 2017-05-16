package com.br.hadoop;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNHashTag extends Configured implements Tool{
	
	public static class TopNMapper extends Mapper<Object, Text, NullWritable, Text>{
		private TreeMap<Integer, Text> topN = new TreeMap<>();

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// (hashtag, count) tuple

			Configuration conf = context.getConfiguration();
			int qtd = Integer.parseInt(conf.get("qtd"));

			String[] words = value.toString().toLowerCase().split("\t") ;
			if (words.length < 2) {
				return;
			}

			topN.put(Integer.parseInt(words[1].toLowerCase()), new Text(value));

			if (topN.size() > qtd) {
				topN.remove(topN.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			for (Text t : topN.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}
	
	public static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> topN = new TreeMap<>();

		@Override
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String[] words = value.toString().toLowerCase().split("\t") ;

				Configuration conf = context.getConfiguration();
				int qtd = Integer.parseInt(conf.get("qtd"));

				topN.put(Integer.parseInt(words[1].toLowerCase()),
						new Text(value));

				if (topN.size() > qtd) {
					topN.remove(topN.firstKey());
				}
			}

			for (Text word : topN.descendingMap().values()) {
				context.write(NullWritable.get(), word);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		conf.set("qtd", args[2]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(TopNHashTag.class);
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TopNHashTag(), args);
		System.exit(exitCode);
	}
}