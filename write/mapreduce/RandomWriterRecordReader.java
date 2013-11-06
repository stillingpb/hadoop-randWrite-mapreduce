package random.write.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;

public class RandomWriterRecordReader implements
		RecordReader<LongWritable, BytesWritable> {
	private static final int RECORD_SIZE = 1024;
	private int recordSize = RECORD_SIZE;

	private Path file;
	private FSDataInputStream in;

	private long start;
	private long end;
	private long pos;

	private byte[] buf = new byte[RECORD_SIZE];

	public RandomWriterRecordReader(Configuration job,
			RandomWriterInputSplit split) throws IOException {
		this.start = split.getStart();
		this.end = split.getLength() + this.start;
		this.pos = start;

		file = split.getPath();
		FileSystem fs = FileSystem.get(job);
		in = fs.open(file);
	}

	@Override
	public boolean next(LongWritable key, BytesWritable value)
			throws IOException {
		if (pos >= end)
			return false;
		if (pos + recordSize > end)
			recordSize = (int) (end - pos);
		in.readFully(pos, buf, 0, recordSize);
		value.set(buf, 0, recordSize);
		key.set(pos);
		pos += recordSize;
		return true;
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public void close() throws IOException {
		if (in != null)
			in.close();
	}

	@Override
	public float getProgress() throws IOException {
		return (pos - start) / (float) (end - start);
	}

}
