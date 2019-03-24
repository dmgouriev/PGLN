package ru.mvnm.pgln;

public class PglnNotification {

    int id;
    int status;
    String eventName;
    String eventData;

    public PglnNotification(int id, int status, String eventName, String eventData) {
        this.id = id;
        this.status = status;
        this.eventName = eventName;
        this.eventData = eventData;
    }

    public int getId() {
        return id;
    }

    public int getStatus() {
        return status;
    }

    public String getEventName() {
        return eventName;
    }

    public String getEventData() {
        return eventData;
    }

    public static PglnNotification of(String jsonString, PglnConfiguration.JsonParser parser) {
        return parser.parse(jsonString);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PglnNotification {\n")
                .append("id = ").append(id).append(",\n")
                .append("status = ").append(status).append(",\n")
                .append("event_name = '").append(eventName).append("',\n")
                .append("event_data = '").append(eventData).append("'\n")
                .append("}");
        return sb.toString();
    }

}
