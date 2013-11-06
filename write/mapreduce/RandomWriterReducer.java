package random.write.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RandomWriterReducer implements
		Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	@Override
	public void configure(JobConf job) {

	}

	@Override
	public void close() throws IOException {

	}

	/**
	 * 不做任何处理的reduce，本可以不要，但是由于不知道在reduce任务为0时，是否会输出文件，所以先保留着
	 */
	@Override
	public void reduce(LongWritable key, Iterator<BytesWritable> values,
			OutputCollector<LongWritable, BytesWritable> output, Reporter reporter)
			throws IOException {
		while (values.hasNext())
			output.collect(key, values.next());
	}
}
