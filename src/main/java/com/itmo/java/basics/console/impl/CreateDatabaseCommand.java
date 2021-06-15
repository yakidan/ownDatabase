package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Команда для создания базы данных
 */
public class CreateDatabaseCommand implements DatabaseCommand {
    private ExecutionEnvironment env;
    private DatabaseFactory factory;
    private List<RespObject> commandArgs;
    private final int numberArgs = 3;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param factory     функция создания базы данных (пример: DatabaseImpl::create)
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя создаваемой бд
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> commandArgs) {
        this.env = env;
        this.factory = factory;
        this.commandArgs = commandArgs;

        if (commandArgs.size() != numberArgs) {
            throw new IllegalArgumentException("Wrong number parameters in CreateDatabaseCommand");
        }
    }

    /**
     * Создает бд в нужном env
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная база была создана. Например, "Database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        Database newDatabase;
        try {
            newDatabase =
                    factory.createNonExistent(commandArgs.
                                    get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex())
                                    .asString(),
                            env.getWorkingPath());
            env.addDatabase(newDatabase);

            return DatabaseCommandResult.success(("Database with name" + newDatabase.getName() + " created").getBytes(StandardCharsets.UTF_8));
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
    }
}
