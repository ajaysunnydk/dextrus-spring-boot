package com.dextrus.demo.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dextrus.demo.entity.ConnectionProperties;
import com.dextrus.demo.entity.RequestBodyPattern;
import com.dextrus.demo.entity.RequestBodyQuery;
import com.dextrus.demo.entity.TableDescription;
import com.dextrus.demo.entity.TableType;
import com.dextrus.demo.service.ConnectionService;

@RestController
@RequestMapping("/dextrus")
public class ConnectionController {

	@Autowired
	private ConnectionService service;

	@GetMapping("/test")
	public ResponseEntity<String> test(){
		return new ResponseEntity<String>("API Tiggered",HttpStatus.OK);
	}
	
	@PostMapping("/connect")
	public ResponseEntity<String> connectToSqlServer(@RequestBody ConnectionProperties properties) {
		Connection connection = service.getSQLServerConnection(properties);
		if(connection==null)
			return new ResponseEntity<String>("Connection Failed", HttpStatus.SERVICE_UNAVAILABLE);
		else
			return new ResponseEntity<String>("Connected to SQL Server", HttpStatus.OK);
	}

	@PostMapping("/")
	public ResponseEntity<List<String>> getCatalogs(@RequestBody ConnectionProperties properties) {
		List<String> catalogs = service.getCatalogsList(properties);
		return new ResponseEntity<List<String>>(catalogs, HttpStatus.OK);
	}

	@PostMapping("/{catalog:.+}")
	public ResponseEntity<List<String>> getSchemas(@PathVariable("catalog") String catalog, @RequestBody ConnectionProperties properties){
		String catalogName = null;
	    try {
	    	catalogName = URLDecoder.decode(catalog, "UTF-8");
	    	System.out.println("--------------"+catalogName+"-----------------");
	    } catch (UnsupportedEncodingException e) {
	        e.printStackTrace();
	    }
		List<String> schemas = service.getSchemasList(properties,catalogName);
		return new ResponseEntity<List<String>>(schemas, HttpStatus.OK);
	}
	
	@PostMapping("/{catalog}/{schema}")
	public ResponseEntity<List<TableType>> getViewsAndTables(@PathVariable String catalog,@PathVariable String schema, @RequestBody ConnectionProperties properties){
		List<TableType> viewsAndTables = service.getTablesAndViews(properties,catalog,schema);
 		return new ResponseEntity<List<TableType>>(viewsAndTables, HttpStatus.OK);
	}
	
	@PostMapping("/{catalog}/{schema}/{table}")
	public ResponseEntity<List<TableDescription>> getColumnProperties(@PathVariable String catalog, @PathVariable String schema, @PathVariable String table, @RequestBody ConnectionProperties properties){
		List<TableDescription> tableDescList = service.getTableDescription(properties, catalog, schema, table);
		return new ResponseEntity<List<TableDescription>>(tableDescList, HttpStatus.OK);	
	}
	
	@PostMapping("/query")
	public ResponseEntity<List<List<Object>>> test(@RequestBody RequestBodyQuery queryBody ) {
		ConnectionProperties prop = queryBody.getProperties();
		String query = queryBody.getQuery();
		List<List<Object>> tableDataList = service.getTableData(prop, query);
		return new ResponseEntity<List<List<Object>>>(tableDataList, HttpStatus.OK);
	}
	
	@PostMapping("/search")
	public ResponseEntity<List<TableType>> getTablesByPattern(@RequestBody RequestBodyPattern bodyPattern){
		List<TableType> viewsAndTables = service.getTablesAndViewsByPattern(bodyPattern.getProperties(),bodyPattern.getCatalog(),bodyPattern.getPattern());
		return new ResponseEntity<List<TableType>>(viewsAndTables, HttpStatus.OK);
	}
	

}
