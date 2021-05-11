package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {

    private Table table;
    private DatabaseCacheImpl cache = new DatabaseCacheImpl();

    public CachingTable(Table table) {
        this.table = table;
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        cache.set(objectKey,objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (cache.get(objectKey) != null) {
            return Optional.of(cache.get(objectKey));
        }
        var key = table.read(objectKey);
        if (key.isPresent()){
            cache.set(objectKey, key.get());
        }
        return table.read(objectKey);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        table.delete(objectKey);
        cache.delete(objectKey);
    }
}
