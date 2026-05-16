package sd2526.trab.impl.zoho.msgs;

public record ZohoEmailItem(
        String messageId,
        String subject,
        String folderId,
        String receivedTime
) {
}