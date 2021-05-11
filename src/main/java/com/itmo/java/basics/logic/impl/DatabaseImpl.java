package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private String dbName;
    private Path databaseRoot;

    private Map<String, Table> tableHashMap = new HashMap<>();

    private DatabaseImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null || databaseRoot == null) {
            throw new DatabaseException("Db Name or databaseRoot is null in DataBase create");
        }

        Path curDatabaseRoot = Paths.get(databaseRoot.toString(), dbName);
        if (!databaseRoot.toFile().exists()) {
            throw new DatabaseException("The directory already exist in Database Create");
        }

        if (!curDatabaseRoot.toFile().exists() && !curDatabaseRoot.toFile().mkdir()) {
            throw new DatabaseException("Doesn't work mkdir() in Database Create");
        }

        return new DatabaseImpl(dbName, Paths.get(databaseRoot.toString(), dbName));
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context.getDbName(),context.getDatabasePath(),context.getTables());
    }

    private DatabaseImpl(String dbName, Path databaseRoot, Map<String, Table> tableHashMap) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        this.tableHashMap = tableHashMap;
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("TableName is null in createTableIfNotExists()");
        }
        if (tableHashMap.containsKey(tableName)) {
            throw new DatabaseException("The table name already exist! in createTableIfNotExists()");
        }
        tableHashMap.put(tableName, new CachingTable(TableImpl.create(tableName, databaseRoot, new TableIndex())));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (!tableHashMap.containsKey(tableName)) {
            throw new DatabaseException("The table name didn't create in write() DatabaseImpl");
        }
        tableHashMap.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (!tableHashMap.containsKey(tableName)) {
            throw new DatabaseException("The table name wasn't  in read() DatabaseImpl");
        }
        return tableHashMap.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (!tableHashMap.containsKey(tableName)) {
            throw new DatabaseException("The table name didn't create in write() DatabaseImpl");
        }
        tableHashMap.get(tableName).delete(objectKey);
    }
}
