package random.write.test;

import org.apache.hadoop.fs.Path;

import random.write.RandomWriteUtil;

public class RandomWriterTest {

	public static void main(String[] args) {
		Path des = new Path("hdfs://localhost:9000/randwrite/random");
		char[] chs = new char[] { '1', '2', '3', '4', '5', '6', '7', '8', '9' };
		byte[] buf = new String(chs).getBytes();
		long pos = 40;
		try {
			RandomWriteUtil.write(buf, des, pos);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
