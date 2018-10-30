package fastwave.demo.hbaseAPI.service;

import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import fastwave.demo.hbaseAPI.hbase.mapper.Person;

public interface PersonService {
	boolean createTable();
	Person findByRowKey(String rowKey);
	List<Person> findByRow(String startRow, String endRow);
	boolean batchSaveOrUpate(List<Mutation> params);
	boolean saveOrUpate(Put put);
	boolean deleteByKey(String key);
}
