package fastwave.demo.hbaseAPI.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import fastwave.demo.hbaseAPI.entity.ResultEntity;
import fastwave.demo.hbaseAPI.hbase.mapper.Person;
import fastwave.demo.hbaseAPI.service.PersonService;

/**
 * @autor cp
 * @Date 2018/10/28
 */
@RequestMapping(value = {"/person/"}, produces="application/json;charset=UTF-8")
@RestController
public class PersonController {

	@Autowired
	private PersonService service;
	
	private static byte[] FAMILY = "person".getBytes();
	private static byte[] NAME = "name".getBytes();
	private static byte[] AGE = "age".getBytes();
	private static byte[] SEX = "sex".getBytes();
	
	@GetMapping(value = "/createtable")
	public ResultEntity createtable() {
		if(service.createTable())
		{
			return new ResultEntity(true,"表创建成功");
		}
		return new ResultEntity(true,"表创建失败");
	}

	@GetMapping(value = "/query")
	public List<Person> query() {
		return service.findByRow("0010", "0015");
	}

	@GetMapping(value = "/update")
	public ResultEntity update() {
		Put put = new Put(Bytes.toBytes("0010"));
		put.addColumn(FAMILY,NAME, Bytes.toBytes("zs" + (new Date()).toString()));
		put.addColumn(FAMILY,AGE, Bytes.toBytes(20  - 10));
		put.addColumn(FAMILY,SEX, Bytes.toBytes("female"));
		
		if(service.saveOrUpate(put))
		{
			return new ResultEntity(true,"修改成功");
		}
		return new ResultEntity(true,"修改失败");
	}

	@GetMapping(value = "/batchadd")
	public ResultEntity batchadd() {
		List<Mutation> list = new ArrayList<Mutation>();
		
		for(int i=0;i<10;i++)
		{
			Put put = new Put(Bytes.toBytes("001" + i));
			put.addColumn(FAMILY,NAME, Bytes.toBytes("zs" + i));
			put.addColumn(FAMILY,AGE, Bytes.toBytes(20 + i));
			put.addColumn(FAMILY,SEX, Bytes.toBytes("male" + i));
			list.add(put);
		}
		
		if(service.batchSaveOrUpate(list))
		{
			return new ResultEntity(true,"批量插入成功");
		}
		return new ResultEntity(true,"批量插入失败");
	}
	
	@GetMapping(value = "/add")
	public ResultEntity add() {
		int i = (int)(Math.random()*100);
		Put put = new Put(Bytes.toBytes("001" + i));
		put.addColumn(FAMILY,NAME, Bytes.toBytes("zs" + i));
		put.addColumn(FAMILY,AGE, Bytes.toBytes(20 + i));
		put.addColumn(FAMILY,SEX, Bytes.toBytes("male" + i));
		
		if(service.saveOrUpate(put))
		{
			return new ResultEntity(true,"批量插入成功");
		}
		return new ResultEntity(false,"批量插入失败");
	}
	

	@GetMapping(value = "/delete")
	public ResultEntity delete() {
		String key = "0011";
		if( service.deleteByKey(key))
		{
			return new ResultEntity(true,"删除成功");
		}
		return new ResultEntity(false,"删除失败");
	}
}
