package flightdata1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class flightdatareducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int intTotal = 0;
		
		for (IntWritable val : values) {
			int intV = val.get();
			intTotal+=intV;
		}
		
		IntWritable iResult = new IntWritable(intTotal);
		
		context.write(_key, iResult);
	}

}
