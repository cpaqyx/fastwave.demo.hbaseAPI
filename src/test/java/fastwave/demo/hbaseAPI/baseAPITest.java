package fastwave.demo.hbaseAPI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import fastwave.demo.hbaseAPI.entity.User;

public class baseAPITest {
	
	private static String TABLENAME = "student";
	private static String FAMILY_INFOMATION = "information";
	private static String FAMILY_CONTACT = "contact";
	private static String QUORUM = "192.168.2.114"; //集群配置示例："192.168.2.114,192.168.2.115,192.168.2.116"
	private static String PORT = "2181";

	private static Admin admin;

	public static void main(String[] args) {
		try {
			// 1.创建表
			createTable(TABLENAME, new String[] { FAMILY_INFOMATION, FAMILY_CONTACT });
			
			// 2.添加记录
			User user = new User("001", "cp", "123456", "man", "30", "13311112222", "362642626@qq.com");
			insertData(TABLENAME, user);
			user = new User("002", "zs", "234567", "female", "28", "13533334444", "362642626@qq.com");
			insertData(TABLENAME, user);
			
			// 3.打印出所有数据
			System.out.println("--------------------3.打印刚插入的两条记录-----------------------");
			printUsers(getAllData(TABLENAME));
			
			// 4.打印原始数据
			System.out.println("--------------------4.打印原始数据-----------------------");
			getNoDealData(TABLENAME);
			
			// 5.按key查询
			System.out.println("--------------------5.根据rowKey查询--------------------");
			User user001 = getDataByRowKey(TABLENAME, "001");
			System.out.println(user001.toString());
			
			// 6.查找某一项记录
			System.out.println("--------------------6.获取指定单条记录的某一个属性值-------------------");
			String user_phone = getCellData(TABLENAME, "001", FAMILY_CONTACT, "phone");
			System.out.println("电话号为：" + user_phone);
			
			// 7.删除数据
			deleteByRowKey(TABLENAME, "001");
			System.out.println("--------------------7.1删除了001，只有002记录了--------------------");
			printUsers(getAllData(TABLENAME));
			
			deleteByRowKey(TABLENAME, "002");
			System.out.println("--------------------7.2全部删除完了，没有记录了--------------------");
			printUsers(getAllData(TABLENAME));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void printUsers(List<User> list)
	{
		if(null == list || 0 == list.size())
		{
			System.out.println("表记录为空");
			return;
		}
		int i = 1;
		for (User item : list) {
			System.out.println("--------------------打印第" + i++ + "条记录--------------------");
			System.out.println(item.toString());
		}
	}

	// 连接集群
	public static Connection initHbase() throws IOException {

		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", PORT);
		configuration.set("hbase.zookeeper.quorum", QUORUM);
		configuration.set("hbase.master", "127.0.0.1:60000");
		Connection connection = ConnectionFactory.createConnection(configuration);
		return connection;
	}

	// 创建表
	public static void createTable(String tableNmae, String[] cols) throws IOException {
		TableName tableName = TableName.valueOf(tableNmae);
		admin = initHbase().getAdmin();
		if (admin.tableExists(tableName)) {
			System.out.println("表已存在！");
		} else {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			for (String col : cols) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			admin.createTable(hTableDescriptor);
		}
	}

	// 插入数据
	public static void insertData(String tableName, User user) throws IOException {
		TableName tablename = TableName.valueOf(tableName);
		Put put = new Put((user.getId()).getBytes());
		put.addColumn("information".getBytes(), "username".getBytes(), user.getUsername().getBytes());
		put.addColumn("information".getBytes(), "age".getBytes(), user.getAge().getBytes());
		put.addColumn("information".getBytes(), "gender".getBytes(), user.getGender().getBytes());
		put.addColumn("contact".getBytes(), "phone".getBytes(), user.getPhone().getBytes());
		put.addColumn("contact".getBytes(), "email".getBytes(), user.getEmail().getBytes());
		Table table = initHbase().getTable(tablename);
		table.put(put);
	}

	// 获取原始数据
	public static void getNoDealData(String tableName) {
		try {
			Table table = initHbase().getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			ResultScanner resutScanner = table.getScanner(scan);
			for (Result result : resutScanner) {
				System.out.println("scan:  " + result);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 根据rowKey进行查询
	public static User getDataByRowKey(String tableName, String rowKey) throws IOException {
		Table table = initHbase().getTable(TableName.valueOf(tableName));
		Get get = new Get(rowKey.getBytes());
		User user = new User();
		user.setId(rowKey);
		// 先判断是否有此条数据
		if (!get.isCheckExistenceOnly()) {
			Result result = table.get(get);
			for (Cell cell : result.rawCells()) {
				String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength());
				String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				if (colName.equals("username")) {
					user.setUsername(value);
				}
				if (colName.equals("age")) {
					user.setAge(value);
				}
				if (colName.equals("gender")) {
					user.setGender(value);
				}
				if (colName.equals("phone")) {
					user.setPhone(value);
				}
				if (colName.equals("email")) {
					user.setEmail(value);
				}
			}
		}
		return user;
	}

	// 查询指定单cell内容
	public static String getCellData(String tableName, String rowKey, String family, String col) {

		try {
			Table table = initHbase().getTable(TableName.valueOf(tableName));
			Get get = new Get(rowKey.getBytes());
			if (!get.isCheckExistenceOnly()) {
				get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
				Result res = table.get(get);
				byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
				return Bytes.toString(resByte);
			} else {
				return "查询结果不存在";
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "出现异常";
	}

	// 查询指定表名中所有的数据
	public static List<User> getAllData(String tableName) {

		Table table = null;
		List<User> list = new ArrayList<User>();
		try {
			table = initHbase().getTable(TableName.valueOf(tableName));
			ResultScanner results = table.getScanner(new Scan());
			User user = null;
			for (Result result : results) {
				System.out.println("用户名:" + new String(result.getRow()));
				user = new User();
				for (Cell cell : result.rawCells()) {
					String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
					String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					user.setId(row);
					if (colName.equals("username")) {
						user.setUsername(value);
					}
					if (colName.equals("age")) {
						user.setAge(value);
					}
					if (colName.equals("gender")) {
						user.setGender(value);
					}
					if (colName.equals("phone")) {
						user.setPhone(value);
					}
					if (colName.equals("email")) {
						user.setEmail(value);
					}
				}
				list.add(user);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	// 删除指定cell数据
	public static void deleteByRowKey(String tableName, String rowKey) throws IOException {
		Table table = initHbase().getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		// 删除指定列
		// delete.addColumns(Bytes.toBytes("contact"), Bytes.toBytes("email"));
		table.delete(delete);
	}

	// 删除表
	public static void deleteTable(String tableName) {
		try {
			TableName tablename = TableName.valueOf(tableName);
			admin = initHbase().getAdmin();
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
