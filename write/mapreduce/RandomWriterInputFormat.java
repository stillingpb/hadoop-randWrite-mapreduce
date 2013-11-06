package random.write.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class RandomWriterInputFormat implements InputFormat<LongWritable, BytesWritable> {

	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		// 只有一个文件
		String infile = job.get("mapred.input.dir", "");
		Path inPath = new Path(infile);
		FileSystem fs = inPath.getFileSystem(job);
		FileStatus status = fs.getFileStatus(inPath);
		if (status.isDir()) {
			// TODO 抛出一个异常
			return null;
		}

		BlockLocation[] blockLocs = fs.getFileBlockLocations(status, 0,
				status.getLen());

		InputSplit[] splits = new InputSplit[blockLocs.length];
		for (int i = 0; i < blockLocs.length; i++) {
			BlockLocation loc = blockLocs[i];
			RandomWriterInputSplit split = new RandomWriterInputSplit(inPath,
					loc.getOffset(), loc.getLength(), loc.getHosts());
			splits[i] = split;
		}
		return splits;
	}

	@Override
	public RecordReader<LongWritable, BytesWritable> getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		return new RandomWriterRecordReader(job, (RandomWriterInputSplit)genericSplit);
	}
}
