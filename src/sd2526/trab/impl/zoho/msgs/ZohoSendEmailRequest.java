package sd2526.trab.impl.zoho.msgs;

public record ZohoSendEmailRequest(
        String fromAddress,
        String toAddress,
        String subject,
        String content
) {
}