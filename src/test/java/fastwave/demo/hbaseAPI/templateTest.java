package fastwave.demo.hbaseAPI;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import fastwave.demo.hbaseAPI.service.PersonService;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class templateTest {
	
	private static byte[] FAMILY = "person".getBytes();
	private static byte[] NAME = "name".getBytes();
	private static byte[] AGE = "age".getBytes();
	private static byte[] SEX = "sex".getBytes();
	
	@Autowired
	private PersonService service;
	
	@Before
	public void init()
	{
		;
	}
	
	@Test
	public void testSaveOrUpdate()
	{
		List<Mutation> list = new ArrayList<Mutation>();
		
		for(int i=0;i<10;i++)
		{
			Put put = new Put(Bytes.toBytes("001" + i));
			put.addColumn(FAMILY,NAME, Bytes.toBytes("zs" + i));
			put.addColumn(FAMILY,AGE, Bytes.toBytes(20 + i));
			put.addColumn(FAMILY,SEX, Bytes.toBytes("male" + i));
			list.add(put);
		}
		service.batchSaveOrUpate(list);
	}
	
	@Test
	public void testFindByRowKey()
	{
		System.out.println(service.findByRowKey("0010"));
	}
	
	@Test
	public void testFindByRows()
	{
		System.out.println(service.findByRow("001","003"));
	}

}
