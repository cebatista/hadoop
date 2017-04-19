import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ContadorHashTagReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context
			) throws IOException, InterruptedException {
		
		int soma = 0;
        for (IntWritable val : values) {
            soma += val.get();
        }
        context.write(key, new IntWritable(soma));
	}
}