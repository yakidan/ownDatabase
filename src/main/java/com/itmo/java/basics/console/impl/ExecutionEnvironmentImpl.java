package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final DatabaseConfig databaseConfig;


    private final Map<String, Optional<Database>> databaseMap = new HashMap<>();


    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.databaseConfig = config;
    }

    @Override
    public Optional<Database> getDatabase(String name) {

        if (!databaseMap.containsKey(name)) {
            return Optional.empty();
        }
        return databaseMap.get(name);

    }

    @Override
    public void addDatabase(Database db) {

        databaseMap.put(db.getName(), Optional.of(db));

    }

    @Override
    public Path getWorkingPath() {
        return Path.of(databaseConfig.getWorkingPath());
    }
}
