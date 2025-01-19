package br.com.example.advancedinsert.dao;

import br.com.example.advancedinsert.dao.connections.MySQLConnection;
import br.com.example.advancedinsert.dao.connections.PostgresConnection;
import br.com.example.advancedinsert.dao.entity.LargeTable;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AdvancedInsertService {

    public List<LargeTable> searchAll() {
        final var query = """
                SELECT id, date FROM large_table
                """;

        try (final var connection = PostgresConnection.getConnection()) {
            final Statement statement;
            final ResultSet rs;
            statement = connection.createStatement();
            rs = statement.executeQuery(query);

            final List<LargeTable> response = new ArrayList<>();

            while (rs.next()) {
                final var id = (UUID) rs.getObject("id");
                final var date = rs.getDate("date");
                response.add(new LargeTable(id, date.toLocalDate()));
            }

            return response;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertIntoMySQL(final List<LargeTable> largeTables) {
        try {
            insert(largeTables);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void insert(final List<LargeTable> largeTables) throws SQLException {
        int numberOfProcessorsAvailable = getRoundedProcessorsAvailable(largeTables);

        final int numberOfListPerThread = largeTables.size() / numberOfProcessorsAvailable;

        final List<List<LargeTable>> slicedList = new ArrayList<>();

        int startIndex = 0;
        int finalIndex = numberOfListPerThread;

        for (int i = 0; i < numberOfProcessorsAvailable; i++) {
            final List<LargeTable> sliced = largeTables.subList(startIndex, finalIndex);
            slicedList.add(sliced);
            startIndex += numberOfListPerThread;
            finalIndex += numberOfListPerThread;
        }

        for (int i = 0; i < slicedList.size(); i++) {
            final int index = i;
            Thread thread = new Thread(() -> prepareRunnable(slicedList.get(index)));
            thread.start();
        }
    }

    private static int getRoundedProcessorsAvailable(final List<?> list) {
        var numberOfProcessorsAvailable = Runtime.getRuntime().availableProcessors();

        while (list.size() % numberOfProcessorsAvailable != 0) {
            --numberOfProcessorsAvailable;
        }

        return numberOfProcessorsAvailable;
    }

    private static void prepareRunnable(final List<LargeTable> largeTables) {
        try (final var connection = MySQLConnection.getConnection()) {
            prepare(largeTables, connection);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void prepare(final List<LargeTable> largeTables,
                                final Connection connection) throws SQLException {
        var preparedStatement = connection.prepareStatement("INSERT INTO large_table (id, date) VALUES (?, ?)");
        connection.setAutoCommit(false);

        int count = 0;

        for (final LargeTable aLargeTable : largeTables) {
            execute(aLargeTable, preparedStatement, count);
        }

        preparedStatement.executeBatch();
        connection.commit();
    }

    private static void execute(final LargeTable aLargeTable,
                                final PreparedStatement preparedStatement,
                                int count) throws SQLException {
        preparedStatement.setString(1, aLargeTable.id().toString());
        preparedStatement.setDate(2, Date.valueOf(LocalDate.from(aLargeTable.date())));
        preparedStatement.addBatch();

        if (++count % 1000 == 0) {
            preparedStatement.executeBatch();
        }
    }
}
