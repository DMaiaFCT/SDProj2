package sd2526.trab.impl;
import sd2526.trab.impl.zoho.msgs.ZohoFolderListReply;
import sd2526.trab.impl.zoho.msgs.ZohoFolder;


import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import sd2526.trab.api.Message;
import sd2526.trab.impl.zoho.ZohoServiceFactory;
import sd2526.trab.impl.zoho.ZohoTokenManager;
import sd2526.trab.impl.zoho.msgs.*;
import sd2526.trab.impl.utils.JSON;

import java.util.Collections;
import java.util.List;

public class Zoho {
    static final String MAIL_API_BASE = "https://mail.zoho.eu/api";

    static final String CLIENT_ID = "1000.OSD1FRO943P8LWDAGY57WAUUPJBG1Z";
    static final String CLIENT_SECRET = "013c57580b5583a9c6dd6b7e6a718bba8f7fc15fa7";
    static final String REFRESH_TOKEN = "1000.3ccecf0c74f9bfa5ff4042db0ced7712.b0b952af0dec11dc0c7e56b554e28095";

    private static final String ACCOUNTS = "/accounts";
    private static final String MESSAGES = "/messages";
    private static final String FOLDERS = "/folders";
    private static final String CONTENT = "/content";
    private static final String VIEW = "/view";

    public static final String SUBJECT_PREFIX = "[SD-MSG]";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;

    static Zoho instance;

    //para não chamar getAccount() em todas as operações
    private String cachedAccountId;

    private String cachedInboxFolderId;

    private Zoho() {
        service = ZohoServiceFactory.buildService(CLIENT_ID, CLIENT_SECRET);
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }

    synchronized public static Zoho getInstance() {
        if (instance == null)
            instance = new Zoho();
        return instance;
    }

    public ZohoAccount getAccount() throws Exception {
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());

        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS);
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var body = response.getBody();
                var data = JSON.decode(body, ZohoAccountReply.class).data();
                if (data == null || data.isEmpty()) return null;
                return data.get(0);
            } else {
                System.err.println("getAccount failed: " + response.getCode() + "/" + response.getBody());
                return null;
            }
        }
    }

    public synchronized String getAccountId() throws Exception {
        if (cachedAccountId == null) {
            var account = getAccount();
            if (account == null) throw new Exception("No Zoho id");
            cachedAccountId = account.accountId();
        }
        return cachedAccountId;
    }

    public synchronized String getInboxFolderId() throws Exception {
        if (cachedInboxFolderId == null) {
            var accountId = getAccountId();
            var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + FOLDERS;

            var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
            OAuthRequest request = new OAuthRequest(Verb.GET, url);
            service.signRequest(accessToken, request);

            try (Response response = service.execute(request)) {
                if (response.isSuccessful()) {
                    var reply = JSON.decode(response.getBody(), ZohoFolderListReply.class);
                    cachedInboxFolderId = reply.data().stream()
                            .filter(f -> "Sent".equals(f.folderType()))
                            .map(ZohoFolder::folderId)
                            .findFirst()
                            .orElseThrow(() -> new Exception("Sent folder not found"));
                } else throw new Exception("getInboxFolderId failed: " + response.getCode() + "/" + response.getBody());
            }
        }
        return cachedInboxFolderId;
    }

    public String sendMessage(Message msg) throws Exception {
        var accountId = getAccountId();
        var account = getAccount();
        if (account == null) return null;

        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + MESSAGES;

        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.POST, url);
        service.signRequest(accessToken, request);

        var payload = new ZohoSendEmailRequest(account.primaryEmailAddress(), account.primaryEmailAddress(),
                SUBJECT_PREFIX + msg.getId(), JSON.encode(msg));

        request.addHeader("Content-Type", "application/json");
        request.setPayload(JSON.encode(payload));

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var reply = JSON.decode(response.getBody(), ZohoSendEmailReply.class);
                if (reply.data() != null) return reply.data().messageId();
            } else System.err.println("sendMessage failed: " + response.getCode() + "/" + response.getBody());
            return null;
        }
    }

    public List<ZohoEmailItem> listEmails() throws Exception {
        var accountId = getAccountId();

        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + MESSAGES + VIEW;

        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, url);
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var reply = JSON.decode(response.getBody(), ZohoEmailListReply.class);
                return reply.data() != null ? reply.data() : Collections.emptyList();
            } else {
                System.err.println("listEmails failed: " + response.getCode() + "/" + response.getBody());
                return Collections.emptyList();
            }
        }
    }

    public Message getEmailContent(String zohoMsgId) throws Exception {
        var accountId = getAccountId();

        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + FOLDERS + "/" + getInboxFolderId() + MESSAGES + "/" + zohoMsgId + CONTENT;

        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, url);
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var reply = JSON.decode(response.getBody(), ZohoEmailContentReply.class);
                if (reply.data() == null || reply.data().content() == null) return null;
                return JSON.decode(reply.data().content(), Message.class);
            } else {
                System.err.println("getEmailContent failed: " + response.getCode() + "/" + response.getBody());
                return null;
            }
        }
    }

    public boolean deleteEmail(String zohoMsgId) throws Exception {
        var accountId = getAccountId();
        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + FOLDERS + "/" + getInboxFolderId() + MESSAGES + "/" + zohoMsgId + "?expunge=true";

        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.DELETE, url);
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) return true;
            else {
                System.err.println("deleteEmail failed: " + response.getCode() + "/" + response.getBody());
                return false;
            }
        }
    }

    public String findZohoMessageId(String systemMessageId) throws Exception {
        var expectedSubject = SUBJECT_PREFIX + systemMessageId;
        return listEmails().stream()
                .filter(item -> expectedSubject.equals(item.subject()))
                .map(ZohoEmailItem::messageId)
                .findFirst()
                .orElse(null);
    }

    //depois fazer um getAccountId e outros metodos para endpoints relevantes, criar records para as respostas (as que necessitam de resposta)


}