package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.*;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var tableContext = context.currentTableContext();
        var segmentContext = context.currentSegmentContext();
        var segmentIndex = segmentContext.getIndex();
        var path = segmentContext.getSegmentPath();

        if (!path.toFile().exists()) {
            throw new DatabaseException("File doesn't exist in SegmentInitializer");
        }

        if (!Files.isReadable(path)) {
            throw new DatabaseException("File's not readable in SegmentInitializer");
        }
        Set<String> hashSetKeys = new HashSet<>();
        try (DatabaseInputStream databaseInputStream = new DatabaseInputStream(new FileInputStream(path.toString()))) {
            Optional<DatabaseRecord> record;
            int offset = 0;
            while (databaseInputStream.available() != 0) {
                record = databaseInputStream.readDbUnit();
                hashSetKeys.add(new String(record.get().getKey()));
                segmentIndex.onIndexedEntityUpdated(new String(record.get().getKey()), new SegmentOffsetInfoImpl(offset));
                offset += record.get().size();
            }

            var newSegmentContext = new SegmentInitializationContextImpl(segmentContext.getSegmentName(), segmentContext.getSegmentPath(), offset, segmentContext.getIndex());
            Segment segment = SegmentImpl.initializeFromContext(newSegmentContext);
            tableContext.updateCurrentSegment(segment);

            for (String s : hashSetKeys) {
                tableContext.getTableIndex().onIndexedEntityUpdated(s, segment);
            }

        } catch (IOException e) {
            throw new DatabaseException("IoException in SegmentInitializer" + e.getMessage(), e);
        }
    }
}
