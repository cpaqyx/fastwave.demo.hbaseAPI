package fastwave.demo.hbaseAPI.entity;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.spring4all.spring.boot.starter.hbase.api.RowMapper;

public class PersonMapper implements RowMapper<Person> {

	private static byte[] FAMILY = "person".getBytes();
	private static byte[] NAME = "name".getBytes();
	private static byte[] AGE = "age".getBytes();
	private static byte[] SEX = "sex".getBytes();
	
	@Override
	public Person mapRow(Result result, int rowNum) throws Exception {
		Person person = new Person();
		person.setRowKey(Bytes.toString(result.getRow()));
		person.setName(Bytes.toString(result.getValue(FAMILY, NAME)));
		person.setAge(Bytes.toInt(result.getValue(FAMILY, AGE)));
		person.setSex(Bytes.toString(result.getValue(FAMILY, SEX)));
		return person;
	}
	
}
