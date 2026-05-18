package sd2526.trab.impl.zoho.msgs;

import java.util.List;

public record ZohoFolderListReply(ZohoStatus status, List<ZohoFolder> data) {
}