package br.com.example.advancedinsert.dao.connections;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLConnection {
    private static final String URL = "jdbc:mysql://localhost:3306/large_database?useSSL=false&serverTimezone=UTC";
    private static final String USER = "mysql";
    private static final String PASSWORD = "mysql@123";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}
