package ru.mvnm.pgln;

import com.impossibl.postgres.api.jdbc.PGConnection;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

class PglnDatabaseAPI {

    private static final Logger logger = Logger.getLogger(PglnDatabaseAPI.class.getName());

    public static void initializeStructure(PGConnection conn, List<String> channels) {
        try (Statement statement = conn.createStatement()) {
            executeQuery(functionCreateQuery, statement, null, true);
            channels.forEach(channel -> {
                executeQuery(String.format(tableCreateQuery, channel), statement, null, true);
                executeQuery(String.format(triggerCreateQuery, channel, channel, channel), statement, null, true);
            });
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "SQL exception while initializeStructure database", e);
        }
    }

    public static void restartListeners(List<String> channels, Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            channels.forEach(channel -> {
                unlisten(channel, st);
                listen(channel, st);
            });
        }
    }

    public static void unlisten(List<String> channels, Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            channels.forEach(channel -> unlisten(channel, st));
        }
    }

    private static void listen(String channel, Statement statement) {
        executeQuery(String.format(listenQuery, channel), statement, "SQL exception while executing listen query");
    }

    private static void unlisten(String channel, Statement statement) {
        executeQuery(String.format(unlistenQuery, channel), statement, "SQL exception while executing unlisten query");
    }

    public static List<PglnNotification> getUnprocessedNotifications(String channel, Connection conn) throws SQLException {
        List<PglnNotification> notifications = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(String.format(unprocessedNotificationsQuery, channel, PglnNotificationStatus.NEW), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    notifications.add(new PglnNotification(
                            rs.getInt(1),
                            rs.getInt(2),
                            rs.getString(3),
                            rs.getString(4)
                    ));
                }
            }
        }
        return notifications;
    }

    public static void notify(String channel, String eventName, String eventData, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            executeQuery(String.format(notifyQuery, channel, PglnNotificationStatus.NEW, eventName, eventData), statement,
                    String.format("exception while executing PglnClient::notify, channel='%s', event_name='%s', event_data='%s'", channel, eventName, eventData)
            );
        }
    }

    public static void notificationInProcess(String channel, int eventId, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            executeQuery(String.format(notificationStatusQuery, channel, PglnNotificationStatus.IN_PROCESS, eventId), statement,
                    String.format("exception while executing PglnClient::notificationInProcess, event_id=%d, channel='%s'", eventId, channel));
        }
    }

    public static void notificationExecuted(String channel, int eventId, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            executeQuery(String.format(notificationStatusQuery, channel, PglnNotificationStatus.EXECUTED, eventId), statement,
                    String.format("exception while executing PglnClient::notificationExecuted, event_id=%d, channel='%s'", eventId, channel));
        }
    }

    private static void executeQuery(String query, Statement statement, String errorMessage) {
        executeQuery(query, statement, errorMessage, false);
    }

    private static void executeQuery(String query, Statement statement, String errorMessage, boolean ignoreException) {
        try {
            statement.execute(query);
        } catch (SQLException e) {
            if (!ignoreException) logger.log(Level.SEVERE, errorMessage, e);
        }
    }

    final static private String tableCreateQuery =
            "create table if not exists %s (\n" +
                    "    id          serial,\n" +
                    "    status      int2,\n" +
                    "    create_time timestamp default current_timestamp,\n" +
                    "    event_name  varchar(255),\n" +
                    "    event_data  text\n" +
                    ")";

    final static private String triggerCreateQuery =
            "CREATE TRIGGER notification_tg_%s AFTER INSERT ON %s\n" +
                    "  FOR EACH ROW EXECUTE PROCEDURE tg_notify_table_insert('%s')";

    final static private String functionCreateQuery =
            "create function tg_notify_table_insert()\n" +
                    "  returns trigger\n" +
                    "  language plpgsql\n" +
                    "as $$\n" +
                    "declare\n" +
                    "  channel text := TG_ARGV[0];\n" +
                    "begin\n" +
                    "  PERFORM (\n" +
                    "    with payload(id, status, event_name, event_data) as (\n" +
                    "      select NEW.id, NEW.status, NEW.event_name, NEW.event_data\n" +
                    "      )\n" +
                    "      select pg_notify(channel, row_to_json(payload)::text) from payload\n" +
                    "  );\n" +
                    "  RETURN NULL;\n" +
                    "end;\n" +
                    "$$";

    final static private String unprocessedNotificationsQuery =
            "select id, status, event_name, event_data::text as event_data from %s where status = %d order by create_time";

    final static private String unlistenQuery = "unlisten %s";

    final static private String listenQuery = "listen %s";

    final static private String notifyQuery = "insert into %s(status, event_name, event_data) values (%d, '%s', '%s')";

    final static private String notificationStatusQuery = "update %s set status = %d where id = %d";


}
