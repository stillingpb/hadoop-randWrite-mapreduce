package random.write.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RandomWriterMapper implements
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {
	private long pos;
	private byte[] buf;

	@Override
	public void configure(JobConf job) {
		pos = job.getLong("random.write.pos", 0);
		// 获取数据文件
		try {
			Path[] file = DistributedCache.getLocalCacheFiles(job);
			if (file == null || file.length != 1)
				// throw exception;
				;
			FileInputStream in = new FileInputStream(file.toString());
			List<Byte> list = new ArrayList<Byte>();
			byte[] buffer = new byte[1024];
			int len;
			while ((len = in.read(buffer, 0, 1024)) != -1) {
				for (int i = 0; i < len; i++)
					list.add(buffer[i]);
			}
			in.close();

			Iterator it = list.iterator();
			buf = new byte[list.size()];
			int i = 0;
			while (it.hasNext())
				buf[i++] = (byte) it.next();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
	}

	/**
	 * 	将src[s2,s2+len] 拷贝到 des[s1, s1+len]
	 * @param des
	 * @param s1
	 * @param src
	 * @param s2
	 * @param len
	 */
	private void copy(byte[] des, int s1, byte[] src, int s2, int len) {
		for (int i = 0; i < len; i++)
			des[s1 + i] = src[s2 + i];
	}

	@Override
	public void map(LongWritable key, BytesWritable value,
			OutputCollector<LongWritable, BytesWritable> output,
			Reporter reporter) throws IOException {
		long offset = key.get();
		if (offset > pos + buf.length || offset + value.getLength() < pos) {
			output.collect(key, value);
			return;
		}
		byte[] valBytes = value.getBytes();
		int valLen = value.getLength();
		int bufLen = buf.length;

		if (offset < pos) {
			if (offset + valLen < pos + bufLen) {
				copy(valBytes, (int) (pos - offset), buf, 0, (int) (valLen
						+ offset - pos));
			} else {
				copy(valBytes, (int) (pos - offset), buf, 0, bufLen);
			}
		} else {
			if (pos + bufLen < offset + valLen) {
				copy(valBytes, 0, buf, (int) (offset - pos), (int) (bufLen
						+ pos - offset));
			} else {
				copy(valBytes, 0, buf, (int) (offset - pos), (int) valLen);
			}
		}

		value.set(valBytes, 0, valLen);
		output.collect(key, value);

		// if (offset < pos) {
		// if (offset + valLen < pos + bufLen) {
		// append(valueChars, 0, (int) (pos - offset));
		// append(bufChars, 0, (int) (valueLen + offset - pos));
		// } else {
		// append(valueChars, 0, (int) (pos - offset));
		// append(bufChars, 0, bufLen);
		// append(valueChars, (int) (pos + bufLen - offset),
		// (int) (offset + valueLen - pos - bufLen));
		// }
		// } else {
		// if (pos + bufLen < offset + valueLen) {
		// append(bufChars, (int) (offset - pos),
		// (int) (bufLen + pos - offset));
		//
		// append(valueChars, (int) (pos + bufLen - offset),
		// (int) (valueLen + offset - pos - bufLen));
		// } else {
		// append(bufChars, (int) (offset - pos), (int) valueLen);
		// }
		// }

	}

}
