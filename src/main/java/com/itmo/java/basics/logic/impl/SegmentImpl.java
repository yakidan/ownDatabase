package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private final long MAX_SIZE = 100000;

    private SegmentIndex segmentIndex = new SegmentIndex();
    private String segmentName;
    private Path path;
    private boolean readOnly = false;
    private long size;


    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        if (segmentName == null || tableRootPath == null) {
            throw new DatabaseException("segmentName == null || tableRootPath == null in Segment create()");
        }
        var path = Paths.get(tableRootPath.toString(), segmentName);

        if (path.toFile().exists()) {
            throw new DatabaseException("Segment is exist in Segment create()");
        }
        try {
            Files.createFile(path);
        } catch (Exception e) {
            throw new DatabaseException("Error create File in Segment create", e);
        }
        return new SegmentImpl(segmentName, path);
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return new SegmentImpl(context.getSegmentName(), context.getSegmentPath(), context.getIndex(), context.getCurrentSize());
    }

    private SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.path = tableRootPath;
    }

    private SegmentImpl(String segmentName, Path tableRootPath, SegmentIndex segmentIndex, long size) {
        this.segmentName = segmentName;
        this.path = tableRootPath;
        this.segmentIndex = segmentIndex;
        this.size = size;
        if (size >= MAX_SIZE) {
            this.readOnly = true;
        }
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        try (DatabaseOutputStream databaseOutputStream = new DatabaseOutputStream(new FileOutputStream(path.toString(), true));) {
            if (isReadOnly()) {
                return false;
            }

            WritableDatabaseRecord record = new SetDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8), objectValue);
            segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(size));
            size += databaseOutputStream.write(record);

            if (size >= MAX_SIZE) {
                readOnly = true;
            }
            return true;
        }
    }


    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        try (DatabaseInputStream databaseInputStream = new DatabaseInputStream(new FileInputStream(path.toString()));) {
            var offset = segmentIndex.searchForKey(objectKey);
            if (offset.isEmpty()) {
                return Optional.empty();
            }
            databaseInputStream.skip(offset.get().getOffset());
            var record = databaseInputStream.readDbUnit();
            if (record.isEmpty() || !record.get().isValuePresented()) {
                return Optional.empty();
            }

            return Optional.of(record.get().getValue());
        }
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        try (DatabaseOutputStream databaseOutputStream = new DatabaseOutputStream(new FileOutputStream(path.toString(), true))) {
            if (isReadOnly())
                return false;

            segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(size));
            WritableDatabaseRecord record = new RemoveDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8));
            size += databaseOutputStream.write(record);

            if (size >= MAX_SIZE) {
                readOnly = true;
            }
            return true;
        }
    }
}
