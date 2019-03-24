package ru.mvnm.pgln;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class PglnService {

    private static PglnService INSTANCE = new PglnService();

    public static PglnService getService() {
        return INSTANCE;
    }

    private PglnService() {
    }

    private static final Logger logger = Logger.getLogger(PglnDatabaseAPI.class.getName());

    private PGConnection connection;
    private PglnConfiguration configuration;
    private PGNotificationListener listener;
    private List<String> channels;

    private ScheduledExecutorService connectionRecoveryService;

    private boolean initialized = false;

    public void start(PglnConfiguration configuration) throws PglnConfigurationException {
        this.configuration = configuration;
        if (configuration.getExecutor() == null || configuration.getJsonParser() == null ||
                configuration.getChannels() == null || configuration.getChannels().isEmpty()) {
            throw new PglnConfigurationException(() -> {
                if (configuration.getExecutor() == null) {
                    return "executor not present";
                } else if (configuration.getJsonParser() == null) {
                    return "json parser not present";
                } else if (configuration.getChannels() == null || configuration.getChannels().isEmpty()) {
                    return "channels not present";
                } else {
                    return "unknown error";
                }
            });
        }
        this.channels = Collections.unmodifiableList(new ArrayList<>(configuration.getChannels()));
        this.connection = configuration.getConnection();
        PglnDatabaseAPI.initializeStructure(connection, channels);
        this.listener = new PGNotificationListener() {
            @Override
            public void notification(int processId, String channelName, String payload) {
                if (initialized) {
                    configuration.getExecutor().proceed(channelName, PglnNotification.of(payload, configuration.getJsonParser()));
                }
            }

            @Override
            public void closed() {
                establishDatabaseConnection();
            }
        };
        initialized = true;
        restart();
    }

    private void restart() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = configuration.getConnection();
            }
            if (connection != null && !connection.isClosed()) {
                connectionEstablished();
                PglnDatabaseAPI.unlisten(channels, connection);
                try {
                    connection.removeNotificationListener(listener);
                } catch (Exception ignored) { /* ignored */ }
                initUnprocessed();
                connection.addNotificationListener(listener);
                PglnDatabaseAPI.restartListeners(channels, connection);
            }
        } catch (SQLException ignored) { /* ignored */ }
    }

    public void stop() {
        initialized = false;
        try {
            PglnDatabaseAPI.unlisten(channels, connection);
            connection.removeNotificationListener(listener);
            connection.close();
        } catch (Exception ignored) { /* ignored */ }
    }

    private void initUnprocessed() {
        channels.forEach(channel -> {
            try {
                List<PglnNotification> channelNotifications = PglnDatabaseAPI.getUnprocessedNotifications(channel, connection);
                channelNotifications.forEach(e -> configuration.getExecutor().proceed(channel, e));
            } catch (SQLException ignored) { /* ignored */ }
        });
    }

    private void establishDatabaseConnection() {
        logger.info("Connection refused. Starting connection recovery service");
        connectionRecoveryService = Executors.newSingleThreadScheduledExecutor();
        connectionRecoveryService.scheduleAtFixedRate(() ->  restart(), 0, 30, TimeUnit.SECONDS);
    }

    private void connectionEstablished() {
        logger.info("Connection established");
        if (connectionRecoveryService != null) {
            connectionRecoveryService.shutdownNow();
        }
    }


}
