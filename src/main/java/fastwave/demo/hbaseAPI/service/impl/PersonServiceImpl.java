package fastwave.demo.hbaseAPI.service.impl;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import com.spring4all.spring.boot.starter.hbase.api.HbaseTemplate;
import com.spring4all.spring.boot.starter.hbase.api.TableCallback;

import fastwave.demo.hbaseAPI.entity.Person;
import fastwave.demo.hbaseAPI.entity.PersonMapper;
import fastwave.demo.hbaseAPI.service.PersonService;

@Service
public class PersonServiceImpl implements PersonService {
	private static String TABLENAME = "person";
	private final HbaseTemplate hbaseTemplate;

	public PersonServiceImpl(HbaseTemplate hbaseTemplate) {
		this.hbaseTemplate = hbaseTemplate;
	}

	@Override
	public boolean createTable() {
		try {
			Connection connection = hbaseTemplate.getConnection();
			Admin admin = connection.getAdmin();
			String tableName = TABLENAME;
			
			// 存在此表则删除，不能用于生产环境
			if (admin.isTableAvailable(TableName.valueOf(tableName))) {
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
			}
			
			HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
			hbaseTable.addFamily(new HColumnDescriptor(tableName));
			admin.createTable(hbaseTable);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	@Override
	public Person findByRowKey(String rowKey) {
		return hbaseTemplate.get(TABLENAME, rowKey, new PersonMapper());
	}

	@Override
	public List<Person> findByRow(String startRowKey, String endRowKey) {
		Scan scan = new Scan(Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey));
		return hbaseTemplate.find(TABLENAME, scan, new PersonMapper());
	}

	@Override
	public boolean batchSaveOrUpate(List<Mutation> params) {
		hbaseTemplate.saveOrUpdates(TABLENAME, params);
		return true;
	}

	@Override
	public boolean saveOrUpate(Put put) {
		hbaseTemplate.saveOrUpdate(TABLENAME, put);
		return true;
	}

	@Override
	public boolean deleteByKey(String key) {
		return hbaseTemplate.execute(TABLENAME, new TableCallback<Boolean>() {
			@Override
			public Boolean doInTable(Table table) throws Throwable {
				boolean flag = false;
				try {
					Delete delete = new Delete(key.getBytes());
					table.delete(delete);
					flag = true;
				} catch (Exception e) {
					e.printStackTrace();
				}
				return flag;
			}
		});
	}
}
