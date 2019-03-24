package ru.mvnm.pgln;

import java.sql.Connection;
import java.sql.SQLException;

public class PglnClient {

    public static void notify(String channel, String eventName, String eventData, Connection conn) throws SQLException {
        PglnDatabaseAPI.notify(channel, eventName, eventData, conn);
    }

    public static void notificationInProcess(String channel, int eventId, Connection conn) throws SQLException {
        PglnDatabaseAPI.notificationInProcess(channel, eventId, conn);
    }

    public static void notificationExecuted(String channel, int eventId, Connection conn) throws SQLException {
        PglnDatabaseAPI.notificationExecuted(channel, eventId, conn);
    }

}