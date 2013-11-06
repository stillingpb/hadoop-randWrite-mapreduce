package random.write;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import random.write.mapreduce.RandomWriterInputFormat;
import random.write.mapreduce.RandomWriterMapper;
import random.write.mapreduce.RandomWriterReducer;

public class RandomWriteUtil {

	private static class RandomWriter implements Tool {
		private Configuration conf;
		private String inputFile;
		private String outputDir;
		/**
		 * 待写入的数据
		 */
		private String cacheFile;
		/**
		 * 从pos位置开始写文件
		 */
		private long pos;

		@Override
		public int run(String[] args) throws Exception {
			JobConf job = new JobConf(conf);
			job.setJarByClass(RandomWriteUtil.class);

			job.setMapperClass(RandomWriterMapper.class);
			job.setReducerClass(RandomWriterReducer.class);

			job.setInputFormat(RandomWriterInputFormat.class);
			job.setOutputFormat(TextOutputFormat.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(BytesWritable.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);

			DistributedCache.addCacheFile(new URI(cacheFile), job);
			job.setLong("random.write.pos", pos);

			job.setNumReduceTasks(1);

			FileInputFormat.setInputPaths(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));

			JobClient.runJob(job);
			return 0;
		}

		@Override
		public void setConf(Configuration conf) {
			this.conf = conf;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		public void setInputFile(String inputFile) {
			this.inputFile = inputFile;
		}

		public void setOutputDir(String outputDir) {
			this.outputDir = outputDir;
		}

		public void setCacheFile(String cacheFile) {
			this.cacheFile = cacheFile;
		}

		public void setPos(long pos) {
			this.pos = pos;
		}
	}

	/**
	 * 将特定文件的部分数据，写入到指定文件的指定位置
	 * 
	 * @param src
	 *            拥有数据的文件
	 * @param s1
	 *            src文件开始位置
	 * @param des
	 *            数据写到的特定文件
	 * @param s2
	 *            数据写到的特定开始位置
	 * @param len
	 *            数据长度
	 */
	public void write(Path src, long s1, Path des, long s2, long len) {
		// TODO 还没有做
	}

	/**
	 * 将特定数据，写入到指定文件的指定位置
	 * 
	 * @param src
	 *            待写入的数据
	 * @param s1
	 *            数据开始位置
	 * @param des
	 *            数据写到的特定文件
	 * @param s2
	 *            数据写到的特定开始位置
	 * @param len
	 *            数据长度
	 * @throws IOException
	 */
	public static void write(byte[] src, long s1, Path des, long s2, long len)
			throws IOException {
		// TODO
	}

	/**
	 * 将数据，写入到文件的指定位置
	 * 
	 * @param src
	 *            待写入的数据
	 * @param des
	 *            数据写到的特定文件
	 * @param pos
	 *            数据写入的位置
	 * @throws Exception
	 */
	public static void write(byte[] src, Path des, long pos) throws Exception {
		String desName = des.getName();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		/* 将要写入的数据放入到hdfs的一个文件中 */
		String cacheFile = TEMP_DIR + "/" + desName + CACHE_SUFFIX;
		fs.mkdirs(new Path(TEMP_DIR));
		Path cachePath = new Path(cacheFile);
		if (fs.exists(cachePath))
			; // TODO 返回创建失败Exception
		FSDataOutputStream out = fs.create(cachePath);
		out.write(src);
		out.close();
		System.out.println(des.toString());

		RandomWriter writer = new RandomWriter();
		writer.setConf(conf);
		writer.setInputFile(des.toString());
		writer.setOutputDir(TEMP_OUTPUT_DIR);
		writer.setCacheFile(cacheFile);
		writer.setPos(pos);

		ToolRunner.run(conf, writer, null);

		fs.delete(des, false);
		fs.rename(new Path(TEMP_OUTPUT_FILE), des);
		// 删除临时文件
		fs.delete(new Path(TEMP_DIR), true);
	}

	private static final String TEMP_DIR = "/tmp/rand/write";
	private static final String CACHE_SUFFIX = ".cache";
	private static final String TEMP_OUTPUT_DIR = TEMP_DIR + "/output";
	private static final String TEMP_OUTPUT_FILE = TEMP_OUTPUT_DIR
			+ "/part-00000";

	public static void main(String[] args) throws Exception {
		// createFile(path, LENGTH);

		// Configuration conf = new Configuration();
		// FileSystem fs = FileSystem.get(conf);
		// Path outputDir = new Path(OUTPUT_DIR);
		// fs.delete(outputDir, true);

		// *******************************8
		// ToolRunner.run(new Configuration(), new RandomFileSystem(),
		// new String[] {});

		// Path outputPath = new Path(outputDir, "/part-00000");
		// fs.delete(path, false);
		// fs.rename(outputPath, path);
		// fs.close();

		// FSDataInputStream in = fs.open(path);
		// long pos = 64 * 1024 * 1024 - 1024;
		//
		// in.seek(pos - 100);
		// BufferedOutputStream out = new BufferedOutputStream(
		// new FileOutputStream(TEMP_FILE));
		// byte[] b = new byte[1 * 1024 * 1024];
		// int r = 0;
		// int count = 0;
		// int len = 2 * 11 * 1024 * 1024;
		// while ((r = in.read(b, 0, b.length)) != 0) {
		// out.write(b, 0, r);
		// count += r;
		// if (count > len)
		// break;
		// }
		// out.close();
		// in.close();
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

		int lineSize = 128;
		String str = "";
		for (int i = 0; i < lineSize; i++)
			str += 'a';
		str += '\n';

		int count = 0;
		while (count < len) {
			out.writeChars(str);
			count += lineSize;
		}

		out.close();
	}

}
