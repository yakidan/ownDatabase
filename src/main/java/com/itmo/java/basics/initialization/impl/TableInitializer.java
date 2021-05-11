package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.*;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class TableInitializer implements Initializer {

    private SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var executionEnvironment = context.executionEnvironment();
        var databaseContext = context.currentDbContext();
        var tableContext = context.currentTableContext();
        var workingPath = tableContext.getTablePath();
        try {
            if (!workingPath.toFile().exists()) {
                throw new DatabaseException("File doesn't exists ");
            }
            if (!Files.isReadable(workingPath)) {
                throw new DatabaseException("File's not readable ");
            }
            if (workingPath.toFile().listFiles() != null) {
                ArrayList<File> list = new ArrayList<File>(Arrays.stream(workingPath.toFile().listFiles()).collect(Collectors.toList()));
                list.sort((o1, o2) -> o1.getName().compareTo(o2.getName()));

                for (File file : list) {
                    if (file.isFile()) {
                        var segmentContext = new SegmentInitializationContextImpl(file.getName(), tableContext.getTablePath(), (int) file.length());
                        var initContext = InitializationContextImpl.builder()
                                .executionEnvironment(executionEnvironment)
                                .currentDatabaseContext(databaseContext)
                                .currentTableContext(tableContext)
                                .currentSegmentContext(segmentContext)
                                .build();
                        segmentInitializer.perform(initContext);
                    }
                }
            }
            databaseContext.addTable(TableImpl.initializeFromContext(tableContext));
        } catch (DatabaseException e) {
            throw new DatabaseException("Error in TableInitializer " + e.getMessage(), e);
        }
    }
}
