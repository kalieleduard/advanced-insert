package br.com.example.advancedinsert.dao;

import br.com.example.advancedinsert.controller.ListUtils;
import br.com.example.advancedinsert.dao.connections.PostgresConnection;
import br.com.example.advancedinsert.dao.entity.LargeTable;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class AdvancedInsertService {

    private static final Logger LOGGER = Logger.getLogger(AdvancedInsertService.class.getName());

    public List<LargeTable> searchAll() {
        final List<LargeTable> response = new ArrayList<>();

        final var query = """
                SELECT id, date FROM large_table
                """;

        try (final var connection = PostgresConnection.getConnection()) {
            final PreparedStatement preparedStatement = connection.prepareStatement(query);
            final ResultSet rs;
            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                final var id = rs.getString("id");
                final var date = rs.getDate("date");
                response.add(new LargeTable(UUID.fromString(id), date.toLocalDate()));
            }
        } catch (final SQLException e) {
            LOGGER.severe("SQL Exception occurred: " + e.getMessage());
        }

        return response;
    }

    public void insertIntoMySQL(final List<LargeTable> largeTables) {
        final var threadsToExecute = 10;
        final List<List<LargeTable>> partitions = ListUtils.partitionList(largeTables, threadsToExecute);

        try (final var executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            for (final List<LargeTable> slice : partitions) {
                executorService.submit(() -> execute(slice));
            }

            executorService.shutdown();
        }
    }

    private static void execute(final List<LargeTable> largeTables) {
        final var sql = "INSERT INTO another_large_table (id, date) VALUES (?, ?)";

        try (final var connection = PostgresConnection.getConnection();
             final var preparedStatement = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);

            for (final LargeTable aLargeTable : largeTables) {
                preparedStatement.setString(1, aLargeTable.id().toString());
                preparedStatement.setDate(2, Date.valueOf(aLargeTable.date()));
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            connection.commit();
        } catch (final SQLException e) {
            LOGGER.severe("SQL Exception occurred: " + e.getMessage());
        }
    }
}
