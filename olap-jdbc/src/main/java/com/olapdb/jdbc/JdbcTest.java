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
//			ResultSet resultSet = state.executeQuery("select caller_num,\"product_name\",\"chanel_id\",\"chanel_key\" from biocloo.\"call\"");
//			ResultSet resultSet = state.executeQuery("select \"duration.count\" from \"biocloo\".\"call\" ");
			ResultSet resultSet = state.executeQuery("select \"year\"||'-'||\"month\"||'-'||\"day\", \"business_name\",  from \"outbound\".\"ocall\" limit 1000");
//			ResultSet resultSet = state.executeQuery("SELECT CAST(FLOOR(CAST(\"year\"||'-'||\"month\"||'-'||\"day\"||' '||\"hour\"||':'||\"minute\"||':00' AS TIMESTAMP) TO MINUTE) AS TIMESTAMP) AS \"__timestamp\",\n       max(\"jietong.count\") AS \"拨打数\",\n       max(\"jietong.sum\") AS \"接通数\",\n       max(\"jietong.avg\") AS \"接通率\"\nFROM outbound.ocall\nWHERE \"year\"||'-'||\"month\"||'-'||\"day\"||' '||\"hour\"||':'||\"minute\"||':00' >= '2020-08-04 00:00:00.000000'\n  AND \"year\"||'-'||\"month\"||'-'||\"day\"||' '||\"hour\"||':'||\"minute\"||':00' < '2020-08-05 00:00:00.000000'\nGROUP BY CAST(FLOOR(CAST(\"year\"||'-'||\"month\"||'-'||\"day\"||' '||\"hour\"||':'||\"minute\"||':00' AS TIMESTAMP) TO MINUTE) AS TIMESTAMP)\nORDER BY \"__timestamp\" ASC\nLIMIT 1000"					);
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
