package com.olapdb.jdbc;

import java.sql.*;
import java.util.Properties;

public class JdbcTest {
	public static void main(String[] args) throws Exception {
		com.olapdb.jdbc.Driver driver = new Driver();
		Properties info = new Properties();
		info.put("user", "ADMIN");
		info.put("password", "KYLIN");
//		info.put("ssl", "true");

		Connection conn = driver.connect("jdbc:olap://localhost:8080/outbound", info);
//		Connection conn = driver.connect("jdbc:biocloo://172.18.9.49:8080/outbound", info);

//		ResultSet catalogs = conn.getMetaData().getCatalogs();
//		System.out.println("CATALOGS");
//		printResultSetMetaData(catalogs);
//		printResultSet(catalogs);
//
//		ResultSet schemas = conn.getMetaData().getSchemas();
//		System.out.println("SCHEMAS");
//		printResultSetMetaData(schemas);
//		printResultSet(schemas);
//
//		ResultSet tables = conn.getMetaData().getTables(null, null, null, null);
//		System.out.println("TABLES");
//		printResultSetMetaData(tables);
//		printResultSet(tables);

		for (int j = 0; j < 1; j++) {
			Statement state = conn.createStatement();
			ResultSet resultSet = state.executeQuery("select caller_num,\"product_name\",\"chanel_id\",\"chanel_key\" from biocloo.\"call\"");
			printResultSetMetaData(resultSet);
			printResultSet(resultSet);


			resultSet.close();
		}

//		catalogs.close();
//		schemas.close();
//		tables.close();
		conn.close();
	}
	private static void printResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		System.out.println("Data:");

		while (rs.next()) {
			StringBuilder buf = new StringBuilder();
			buf.append("[");
			for (int i = 0; i < meta.getColumnCount(); i++) {
				if (i > 0)
					buf.append(", ");
				buf.append(rs.getString(i + 1));
			}
			buf.append("]");
			System.out.println(buf);
		}
	}
	private static void printResultSetMetaData(ResultSet rs) throws SQLException {
		ResultSetMetaData metadata = rs.getMetaData();
		System.out.println("Metadata:");

		for (int i = 0; i < metadata.getColumnCount(); i++) {
			String metaStr = metadata.getCatalogName(i + 1) + " " + metadata.getColumnClassName(i + 1) + " "
					+ metadata.getColumnDisplaySize(i + 1) + " " + metadata.getColumnLabel(i + 1) + " "
					+ metadata.getColumnName(i + 1) + " " + metadata.getColumnType(i + 1) + " "
					+ metadata.getColumnTypeName(i + 1) + " " + metadata.getPrecision(i + 1) + " "
					+ metadata.getScale(i + 1) + " " + metadata.getSchemaName(i + 1) + " "
					+ metadata.getTableName(i + 1);
			System.out.println(metaStr);
		}
	}
}
