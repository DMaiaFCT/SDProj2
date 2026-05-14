package sd2526.trab.impl.zoho.msgs;

import java.util.List;

public record ZohoAccountReply(ZohoStatus status, List<ZohoAccount> data) {

}
