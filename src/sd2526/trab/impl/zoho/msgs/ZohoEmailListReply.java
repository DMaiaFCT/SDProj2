package sd2526.trab.impl.zoho.msgs;

import java.util.List;

public record ZohoEmailListReply(ZohoStatus status, List<ZohoEmailItem> data) {
}