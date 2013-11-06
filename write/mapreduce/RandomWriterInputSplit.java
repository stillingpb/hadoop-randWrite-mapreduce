package random.write.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

public class RandomWriterInputSplit implements InputSplit {
	private Path file;
	private long start;
	private long length;
	private String[] hosts;

	public RandomWriterInputSplit() {
	}

	public RandomWriterInputSplit(Path file, long start, long length,
			String[] hosts) {
		this.file = file;
		this.start = start;
		this.length = length;
		this.hosts = hosts;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// 尼玛，原来是这里错了，把file.toString()搞成file.getName()了
		// out.writeUTF(file.getName());
		out.writeUTF(file.toString());

		out.writeLong(start);
		out.writeLong(length);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		file = new Path(in.readUTF());
		start = in.readLong();
		length = in.readLong();
		hosts = null;
	}

	@Override
	public long getLength() throws IOException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException {
		return hosts;
	}

	public Path getPath() {
		return file;
	}

	public long getStart() {
		return start;
	}

}
