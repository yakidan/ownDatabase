package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DatabaseInitializer implements Initializer {

    private TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        var executionEnvironment = initialContext.executionEnvironment();
        var databaseContext = initialContext.currentDbContext();
        var workingPath = databaseContext.getDatabasePath();
        try {
            if (!workingPath.toFile().exists()) {
                throw new DatabaseException("File doesn't exists");
            }
            if (!Files.isReadable(workingPath)) {
                throw new DatabaseException("File's not readable ");
            }

            for (File file : workingPath.toFile().listFiles()) {
                if (file.isDirectory()) {
                    var tableContext = new TableInitializationContextImpl(file.getName(), workingPath, new TableIndex());
                    var initContext = InitializationContextImpl.builder()
                            .executionEnvironment(executionEnvironment)
                            .currentDatabaseContext(databaseContext)
                            .currentTableContext(tableContext)
                            .build();
                    tableInitializer.perform(initContext);
                }
            }
            executionEnvironment.addDatabase(DatabaseImpl.initializeFromContext(databaseContext));
        } catch (DatabaseException e) {
            throw new DatabaseException("Error in DatabaseInitializer " + e.getMessage(), e);
        }
    }
}
