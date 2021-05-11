package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.initialization.TableInitializationContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;


public class TableImpl implements Table {
    private String tableName;
    private Path path;
    private TableIndex tableIndex;
    private Segment curSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        this.tableName = tableName;
        this.path = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
    }

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Path curPathToDatabaseRoot = Paths.get(pathToDatabaseRoot.toString(), tableName);
        if (!pathToDatabaseRoot.toFile().exists()) {
            throw new DatabaseException("The directory already exist in Table Create");
        }
        if (!curPathToDatabaseRoot.toFile().exists() && !curPathToDatabaseRoot.toFile().mkdir()) {
            throw new DatabaseException("Doesn't work mkdir() in Table Create");
        }

        return new TableImpl(tableName, curPathToDatabaseRoot, tableIndex);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new TableImpl(context.getTableName(), context.getTablePath(), context.getTableIndex(), context.getCurrentSegment());
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex, Segment curSegment) {
        this.tableName = tableName;
        this.path = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
        this.curSegment = curSegment;
    }


    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (curSegment == null || curSegment.isReadOnly()) {
            String segmentName = SegmentImpl.createSegmentName(tableName);
            curSegment = SegmentImpl.create(segmentName, path);
        }
        try {
            curSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, curSegment);
        } catch (IOException e) {
            throw new DatabaseException("Error in write in TableImpl", e);
        }

    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var segment = tableIndex.searchForKey(objectKey);
        try {
            if (segment.isPresent()) {
                return segment.get().read(objectKey);
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new DatabaseException("Error in read in TableImpl", e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (!tableIndex.searchForKey(objectKey).isPresent())
            throw new DatabaseException("wrong objectKey in delete TableImpl");
        try {
            if (!curSegment.delete(objectKey)) {
                String segmentName = SegmentImpl.createSegmentName(tableName);
                curSegment = SegmentImpl.create(segmentName, path);
                curSegment.delete(objectKey);
            }
            tableIndex.onIndexedEntityUpdated(objectKey, curSegment);

        } catch (IOException e) {
            throw new DatabaseException("Error in delete in TableImpl", e);
        }
    }
}
