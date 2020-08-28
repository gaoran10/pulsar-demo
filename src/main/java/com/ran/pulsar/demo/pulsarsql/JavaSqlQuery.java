package com.ran.pulsar.demo.pulsarsql;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class JavaSqlQuery {

    public void query() throws SQLException {
        String url = String.format("jdbc:presto://%s",  "localhost:8081");
        Connection connection = DriverManager.getConnection(url, "test", null);

        String query = String.format("select * from pulsar" +
                ".\"public/default\".%s order by __publish_time__ asc", "\"xphone-info-topic\"");
        log.info("Executing query: {}", query);
        ResultSet res = connection.createStatement().executeQuery(query);

        while (res.next()) {
            ResultSetMetaData rsmd = res.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = res.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }

    private static void printCurrent(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print(",  ");
            String columnValue = rs.getString(i);
            System.out.print(columnValue + " " + rsmd.getColumnName(i));
        }
        System.out.println("");
    }

    public static void main(String[] args) throws SQLException {
        JavaSqlQuery sqlQuery = new JavaSqlQuery();
        sqlQuery.query();
    }

}
