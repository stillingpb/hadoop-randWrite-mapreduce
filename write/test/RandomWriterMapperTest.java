package random.write.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import random.write.mapreduce.RandomWriterMapper;

public class RandomWriterMapperTest {

	@Test
	public void test1() throws IOException {
		String value = "aaaaa";
		long offset = 10;
		String buf = "bbb";
		int pos = 9;
		String result = "bbaaa";

		testMapper(value, offset, buf, pos, result);
	}
	
	@Test
	public void test2() throws IOException {
		String value = "aaaaa";
		long offset = 10;
		String buf = "bbbbbbb";
		int pos = 9;
		String result = "bbbbb";

		testMapper(value, offset, buf, pos, result);
	}
	
	@Test
	public void test3() throws IOException {
		String value = "aaaaa";
		long offset = 10;
		String buf = "bb";
		int pos = 11;
		String result = "abbaa";

		testMapper(value, offset, buf, pos, result);
	}
	
	@Test
	public void test4() throws IOException {
		String value = "aaaaa";
		long offset = 10;
		String buf = "bbbbb";
		int pos = 11;
		String result = "abbbb";

		testMapper(value, offset, buf, pos, result);
	}

	private void testMapper(String value, long offset, String buf, int pos,
			String result) throws IOException {
		RandomWriterMapper mapper = new RandomWriterMapper();
		mapper.setPos(pos);
		mapper.setBuf(buf);

		OutputCollector<LongWritable, Text> output = mock(OutputCollector.class);
		mapper.map(new LongWritable(offset), new Text(value), output, null);

		verify(output).collect(new LongWritable(offset), new Text(result));
	}
}
