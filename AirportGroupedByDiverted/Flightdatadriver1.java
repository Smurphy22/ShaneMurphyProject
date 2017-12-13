package flightdata1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Flightdatadriver1 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "FlightDataQuery");
		
		job.setJarByClass(Flightdatadriver1.class);
		
		job.setMapperClass(flightdatamapper1.class);

		job.setReducerClass(flightdatareducer1.class);

		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// specify input and output DIRECTORIES (Flight Data)
		FileInputFormat.setInputPaths(job , new Path("hdfs://localhost:54310/user/hduser/AirportVSDiverted"));
		FileOutputFormat.setOutputPath(job , new Path("hdfs://localhost:54310/user/hduser/AirportVSDiverted/output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
