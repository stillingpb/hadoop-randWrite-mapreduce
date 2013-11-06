package random.write;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class TextRandomWriter {
	private static final String FILEPATH = "/random";
	private static final Path path = new Path(FILEPATH);
	private static final long LENGTH = 128 * 1024 * 1024;

	private static final String TEMP_FILE = "/home/pb/random";

	public static void main(String[] args) throws Exception {
		// createFile(path, LENGTH);

		long pos = 64 * 1024 * 1024 - 1024;
		char[] buf = new char[2 * 1024];
		int offset = 0;

//		new RandomFileSystem().write(path, pos, buf, offset, buf.length);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(path);

		in.seek(pos - 100);
		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(TEMP_FILE));
		byte[] b = new byte[1 * 1024 * 1024];
		int r = 0;
		int count = 0;
		int len = 2 * 11 * 1024 * 1024;
		while ((r = in.read(b, 0, b.length)) != 0) {
			out.write(b, 0, r);
			count += r;
			if (count > len)
				break;
		}
		out.close();
		in.close();
	}

	// public static void main(String args[]) throws Exception{
	// Configuration conf = new Configuration();
	// ToolRunner.run(conf, new RandomFileSystem(), new String[] {});
	//
	// // Path outputPath = new Path(outputDir, "/part-00000");
	// // fs.delete(path, false);
	// // fs.rename(outputPath, path);
	// // fs.close();
	// }

	static void createFile(Path path, long len) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(path);

		int size = (int) (len / 100);
		int last = (int) (len % 100);
		StringBuilder sb = new StringBuilder(size);
		for (int i = 0; i < size; i++)
			sb.append('a');
		String str = sb.toString();

		for (int i = 0; i < 100; i++)
			out.writeChars(str);

		for (int i = 0; i < last; i++)
			out.writeChar('a');

		out.close();
	}
}
