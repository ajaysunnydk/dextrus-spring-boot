package com.dextrus.demo.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.dextrus.demo.common.CC;
import com.dextrus.demo.entity.ConnectionProperties;
import com.dextrus.demo.entity.TableDescription;
import com.dextrus.demo.entity.TableType;

@Service
public class ConnectionService {

	public Connection getSQLServerConnection(ConnectionProperties properties) {
		Connection connection = CC.getConnection(properties);
		return connection;
	}

	public List<String> getCatalogsList(ConnectionProperties properties) {
		List<String> catalogs = null;
		Connection connection = CC.getConnection(properties);
		try {
			ResultSet rs = connection.createStatement().executeQuery("SELECT name FROM sys.databases");
			catalogs = new ArrayList<>();
			while (rs.next()) {
				catalogs.add(rs.getString("name"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return catalogs;
	}

	public List<String> getSchemasList(ConnectionProperties properties, String catalog) {
		List<String> schemas = null;
		try {
			Connection connection = CC.getConnection(properties);
			String query = "SELECT name FROM \"" + catalog + "\".sys.schemas";
			ResultSet rs = connection.createStatement().executeQuery(query);
			schemas = new ArrayList<>();
			while (rs.next())
				schemas.add(rs.getString("name"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return schemas;
	}

	public List<TableType> getTablesAndViews(ConnectionProperties properties, String catalog, String schema) {
		List<TableType> viewsAndTables = new ArrayList<>();
		try {
			Connection connection = CC.getConnection(properties);
			PreparedStatement statement = connection.prepareStatement("use " + catalog + "; " + CC.GET_TABLES_QUERY);
			statement.setString(1, catalog);
			statement.setString(2, schema);
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				TableType tableType = new TableType();
				tableType.setTable_name(resultSet.getString("TABLE_NAME"));
				tableType.setTable_type(resultSet.getString("TABLE_TYPE"));
				viewsAndTables.add(tableType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return viewsAndTables;
	}

	public List<TableDescription> getTableDescription(ConnectionProperties properties, String catalog, String schema,
			String table) {
		List<TableDescription> tableDescList = new ArrayList<>();
		try {
			Connection connection = CC.getConnection(properties);
			PreparedStatement statement = connection.prepareStatement("use " + catalog + "; " + CC.DESCRIPTION_QUERY);
			table = schema + "." + table;
			statement.setString(1, table);
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				TableDescription td = new TableDescription();
				td.setColumnName(resultSet.getString("COLUMN_NAME"));
				td.setDataType(resultSet.getString("DATA_TYPE"));
				td.setPrecision(resultSet.getInt("PRECISION"));
				td.setMaxlength(resultSet.getInt("MAX_LENGTH"));
				td.setIsNullable(resultSet.getInt("IS_NULLABLE"));
				td.setPrimaryKey(resultSet.getInt("PRIMARY_KEY"));
				tableDescList.add(td);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tableDescList;
	}

//	public List<TableDescription> getTableDescription(ConnectionProperties properties, String catalog, String schema,
//			String table) {
//		List<TableDescription> descList = new ArrayList<>();
//		try {
//			Connection connection = CC.getConnection(properties);
//			PreparedStatement stmt = connection
//					.prepareStatement("USE " + catalog + "; " + "SELECT * FROM " + schema + "." + table);
//			ResultSet rs = stmt.executeQuery();
//			ResultSetMetaData meta = rs.getMetaData();
//			DatabaseMetaData meta1 = connection.getMetaData();
//			int columnCount = meta.getColumnCount();
//
//			for (int i = 1; i <= columnCount; i++) {
//				TableDescription td = new TableDescription();
//				td.setColumnName(meta.getColumnName(i));
//				td.setDataType(meta.getColumnTypeName(i));
//				td.setIsNullable(meta.isNullable(i));
//				td.setPrimaryKey(0);
//				td.setMaxlength(meta.getColumnDisplaySize(i));
//				td.setPrecision(meta.getPrecision(i));
//				descList.add(td);
//			}
//			return descList;
//		} catch (Exception e) {
//			e.printStackTrace();
//			return descList;
//		}
//	}

	public List<List<Object>> getTableData(ConnectionProperties properties, String query) {
		List<List<Object>> rows = new ArrayList<>();
		try {
			Connection con = CC.getConnection(properties);
			Statement statement = con.createStatement();
			ResultSet resultSet = statement.executeQuery(query);
			ResultSetMetaData meta = resultSet.getMetaData();
			int columnCount = meta.getColumnCount();
			while (resultSet.next()) {

				List<Object> row = new ArrayList<>();
				for (int i = 1; i <= columnCount; i++) {

					String columnName = meta.getColumnName(i);
					String columnType = meta.getColumnTypeName(i);
					switch (columnType) {
					case "varchar": {
						row.add(columnName + " : " + resultSet.getString(columnName));
						break;
					}
					case "float": {
						row.add(columnName + " : " + resultSet.getFloat(columnName));
						break;
					}
					case "boolean": {
						row.add(columnName + " : " + resultSet.getBoolean(columnName));
						break;
					}
					case "int": {
						row.add(columnName + " : " + resultSet.getInt(columnName));
						break;
					}
					case "timestamp": {
						row.add(columnName + " : " + resultSet.getTimestamp(columnName));
						break;
					}
					case "decimal": {
						row.add(columnName + " : " + resultSet.getBigDecimal(columnName));
						break;
					}
					case "date": {
						row.add(columnName + " : " + resultSet.getDate(columnName));
						break;
					}
					default:
						row.add("!-!-! " + columnName + " : " + resultSet.getObject(columnName));
						System.out.println("Datatype Not available for Column: " + columnName);
					}
				}
				rows.add(row);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rows;
	}

	public List<TableType> getTablesAndViewsByPattern(ConnectionProperties properties, String catalog, String pattern) {
		List<TableType> viewsAndTables = new ArrayList<>();
		try {
			Connection connection = CC.getConnection(properties);
			PreparedStatement statement = connection
					.prepareStatement("use " + catalog + "; " + CC.GET_TABLES_BY_PATTERN_QUERY);
			statement.setString(1, pattern);
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				TableType tableType = new TableType();
				tableType.setTable_name(resultSet.getString("TABLE_NAME"));
				tableType.setTable_type(resultSet.getString("TABLE_TYPE"));
				viewsAndTables.add(tableType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return viewsAndTables;
	}

	public List<Map<String, Object>> getCountRowsFromTable(ConnectionProperties properties, String catalog,
			String schema, String table, int count, Class<?> pojoClass) {
		List<Map<String, Object>> rows = new ArrayList<>();
		try {
			Connection con = CC.getConnection(properties);
			Statement statement = con.createStatement();
			String query = "use " + catalog + "; SELECT TOP " + count + " * FROM " + schema + "." + table;
			ResultSet rs = statement.executeQuery(query);
			ResultSetMetaData meta = rs.getMetaData();
			int columnCount = meta.getColumnCount();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				for (int i = 1; i <= columnCount; i++) {
					String columnName = meta.getColumnName(i);
					String propertyName = columnNameToPropertyName(columnName);
					Object value = rs.getObject(i);
					row.put(propertyName, value);
				}
				rows.add(row);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rows;
	}

//Helper method to convert column names to property names
	private static String columnNameToPropertyName(String columnName) {
		String[] parts = columnName.split("_");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < parts.length; i++) {
			sb.append(capitalize(parts[i]));
		}
		return sb.toString();
	}

//Helper method to capitalize the first letter of a string
	private static String capitalize(String s) {
		return s.substring(0, 1).toUpperCase() + s.substring(1);
	}

}
