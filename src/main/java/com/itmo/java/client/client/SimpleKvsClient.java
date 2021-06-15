package com.itmo.java.client.client;

import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.Objects;
import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final String databaseName;
    private Supplier<KvsConnection> connectionSupplier;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connectionSupplier = Objects.requireNonNull(connectionSupplier);
    }

    private void check(String... params) throws DatabaseExecutionException {
        for (var p : params) {
            if (p == null) {
                throw new DatabaseExecutionException("One of params is null");
            }
        }
    }

    private String getResp(KvsCommand command) throws DatabaseExecutionException, ConnectionException {
        RespObject respObject = connectionSupplier.get().send(command.getCommandId(), command.serialize());
        if (respObject.isError()) {
            throw new DatabaseExecutionException("Resp object is error");
        }
        return respObject.asString();
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        check(databaseName);
        String response;
        try {
            KvsCommand command = new CreateDatabaseKvsCommand(databaseName);
            response = getResp(command);
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error in getting response in CreateDatabaseCommand");
        }
        return response;
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        check(databaseName, tableName);
        String response;
        try {
            KvsCommand command = new CreateTableKvsCommand(databaseName, tableName);
            response = getResp(command);
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error in getting response in CreateTableCommand", e);
        }
        return response;
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        check(databaseName, tableName, key);
        String response;
        try {
            KvsCommand command = new GetKvsCommand(databaseName, tableName, key);
            response = getResp(command);
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error in getting response in GetCommand", e);
        }
        return response;
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        check(databaseName, tableName, key, value);
        String response;
        try {
            KvsCommand command = new SetKvsCommand(databaseName, tableName, key, value);
            response = getResp(command);
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error in getting response in SetCommand", e);
        }
        return response;
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        check(databaseName, tableName, key);
        String response;
        try {
            KvsCommand command = new DeleteKvsCommand(databaseName, tableName, key);
            response = getResp(command);
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error in getting response in DeleteCommand", e);
        }
        return response;
    }
}
