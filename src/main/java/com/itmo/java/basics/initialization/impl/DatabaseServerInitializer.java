package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DatabaseServerInitializer implements Initializer {

   private DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var executionEnvironment = context.executionEnvironment();
        var workingPath = executionEnvironment.getWorkingPath();
        try {
            if (!workingPath.toFile().exists()) {
                if (!workingPath.toFile().mkdir()) {
                    throw new DatabaseException("Directory didn't created ");
                }
            }
            if (!Files.isReadable(workingPath)) {
                throw new DatabaseException("File's not readable ");
            }

            for (File file : workingPath.toFile().listFiles()) {
                if (file.isDirectory()) {
                    var databaseContext = new DatabaseInitializationContextImpl(file.getName(), workingPath);
                    var initContext = InitializationContextImpl.builder()
                            .executionEnvironment(executionEnvironment)
                            .currentDatabaseContext(databaseContext)
                            .build();
                    databaseInitializer.perform(initContext);
                }
            }
        } catch (DatabaseException e) {
            throw new DatabaseException("Error in DatabaseServerInitializer " + e.getMessage(), e);
        }
    }
}
