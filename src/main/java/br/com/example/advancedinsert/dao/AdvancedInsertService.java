package br.com.example.advancedinsert.dao;

import br.com.example.advancedinsert.dao.connections.MySQLConnection;
import br.com.example.advancedinsert.dao.connections.PostgresConnection;
import br.com.example.advancedinsert.dao.entity.LargeTable;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class AdvancedInsertService {

    private static final Logger LOGGER = Logger.getLogger(AdvancedInsertService.class.getName());

    public static final int CONFIGURED_BATCH_SIZE = 1000;

    public List<LargeTable> searchAll() {
        final var query = """
                SELECT id, date FROM large_table
                """;

        try (final var connection = PostgresConnection.getConnection()) {
            final PreparedStatement preparedStatement = connection.prepareStatement(query);
            final ResultSet rs;
            rs = preparedStatement.executeQuery();

            final List<LargeTable> response = new ArrayList<>();

            while (rs.next()) {
                final var id = (UUID) rs.getObject("id");
                final var date = rs.getDate("date");
                response.add(new LargeTable(id, date.toLocalDate()));
            }

            return response;
        } catch (final SQLException e) {
            LOGGER.severe("SQL Exception occurred: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void insertIntoMySQL(final List<LargeTable> largeTables) {
        try {
            insert(largeTables);
        } catch (SQLException e) {
            LOGGER.severe("SQL Exception occurred: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void insert(final List<LargeTable> largeTables) throws SQLException {
        final int numberOfProcessorsAvailable = Runtime.getRuntime().availableProcessors();
        final List<List<LargeTable>> partitions = partitionList(largeTables, numberOfProcessorsAvailable);

        try (final var executorService = Executors.newFixedThreadPool(numberOfProcessorsAvailable)) {
            List<Callable<Void>> tasks = new ArrayList<>();

            for (final List<LargeTable> slice : partitions) {
                tasks.add(() -> {
                    prepareRunnable(slice);
                    return null;
                });
            }

            List<Future<Void>> futures = executorService.invokeAll(tasks);

            for (final Future<Void> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    LOGGER.severe("SQL Exception occurred: " + e.getMessage());
                    throw new RuntimeException("Error while inserting data: ", e.getCause());
                }
            }
        } catch (InterruptedException e) {
            LOGGER.severe("SQL Exception occurred: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static List<List<LargeTable>> partitionList(final List<LargeTable> largeTables,
                                                        final int partitions) {
        final List<List<LargeTable>> result = new ArrayList<>();
        int totalSize = largeTables.size();
        int baseSize = totalSize / partitions;
        int remainder = totalSize % partitions;

        int start = 0;
        for (int i = 0; i < partitions; i++) {
            int chunkSize = baseSize + (remainder > 0 ? 1 : 0);
            remainder = Math.max(0, remainder - 1);

            int end = start + chunkSize;
            end = Math.min(end, totalSize);

            if (start >= end) {
                break;
            }

            result.add(largeTables.subList(start, end));
            start = end;
        }

        return result;
    }

    private static void prepareRunnable(final List<LargeTable> largeTables) {
        try (final var connection = MySQLConnection.getConnection()) {
            prepare(largeTables, connection);
        } catch (final SQLException e) {
            LOGGER.severe("SQL Exception occurred: " + e.getMessage());
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
            count++;
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

        if (count % CONFIGURED_BATCH_SIZE == 0) {
            preparedStatement.executeBatch();
        }
    }
}
