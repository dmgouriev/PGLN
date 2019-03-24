package ru.mvnm.pgln;

import com.impossibl.postgres.api.jdbc.PGConnection;

import java.util.Set;

public interface PglnConfiguration {

    PGConnection getConnection();
    Executor getExecutor();
    JsonParser getJsonParser();
    Set<String> getChannels();

    interface Executor {
        void proceed(String channelName, PglnNotification notification);
    }

    interface JsonParser {
        PglnNotification parse(String jsonString);
    }

}

