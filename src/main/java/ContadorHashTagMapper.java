import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ContadorHashTagMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private final static IntWritable numeroUm = new IntWritable(1);
	private Text palavra = new Text();

	private String hashTagExpRegular = "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)";

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] palavras = value.toString().split(" ");

		for (String str : palavras) {
			if (str.matches(hashTagExpRegular)) {
				palavra.set(str);
				context.write(palavra, numeroUm);
			}
		}
	}
}