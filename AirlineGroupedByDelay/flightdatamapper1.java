package flightdata1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class flightdatamapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		String inString = ivalue.toString();
		
		String[] strArr = inString.split(",");
		
		String field2 = strArr[1];
		String field3 = strArr[2];
		
		int iField3 = Integer.parseInt(field3);
		
		Text outText = new Text(field2);
		IntWritable outInt = new IntWritable(iField3);
		
		context.write(outText, outInt);
		
	}

}
