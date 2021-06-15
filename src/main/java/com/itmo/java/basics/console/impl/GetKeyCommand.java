package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для чтения данных по ключу
 */
public class GetKeyCommand implements DatabaseCommand {
    private ExecutionEnvironment env;
    private List<RespObject> commandArgs;
    private final int numberArgs=5;
    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public GetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.env = env;
        this.commandArgs = commandArgs;

        if (commandArgs.size() != numberArgs) {
            throw new IllegalArgumentException("Wrong number parameters in GetKeyCommand");
        }
    }

    /**
     * Читает значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с прочитанным значением. Например, "previous". Null, если такого нет
     */
    @Override
    public DatabaseCommandResult execute() {
        Optional<Database> database = env.getDatabase(
                commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString()
        );
        if (!database.isPresent()) {
            return DatabaseCommandResult.error("Doesn't find the database" +
                    commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString()
            );
        }
        Optional<byte[]> result;
        try {
            result = database.get().read(
                    commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString(),
                    commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString()
            );
            return DatabaseCommandResult.success(result.get());
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }


    }
}
