package sd2526.trab.impl;

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
import java.util.logging.Logger;

public class Zoho {
    private static final Logger Log = Logger.getLogger(Zoho.class.getName());

    static final String MAIL_API_BASE = "https://mail.zoho.eu/api";
    static final String CLIENT_ID = "1000.OSD1FRO943P8LWDAGY57WAUUPJBG1Z";
    static final String CLIENT_SECRET = "013c57580b5583a9c6dd6b7e6a718bba8f7fc15fa7";
    static final String REFRESH_TOKEN = "1000.3ccecf0c74f9bfa5ff4042db0ced7712.b0b952af0dec11dc0c7e56b554e28095";

    private static final String ACCOUNTS = "/accounts";
    private static final int PAGE_LIMIT = 200;

    public static final String SUBJECT_PREFIX = "[SD-MSG]";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;
    static Zoho instance;

    private String cachedAccountId;
    private String cachedSentFolderId;

    private Zoho() {
        service = ZohoServiceFactory.buildService(CLIENT_ID, CLIENT_SECRET);
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }

    synchronized public static Zoho getInstance() {
        if (instance == null) instance = new Zoho();
        return instance;
    }

    public ZohoAccount getAccount() throws Exception {
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS);
        service.signRequest(accessToken, request);
        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var data = JSON.decode(response.getBody(), ZohoAccountReply.class).data();
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
            if (account == null) throw new Exception("Cannot get Zoho account");
            cachedAccountId = account.accountId();
        }
        return cachedAccountId;
    }

    /**
     * Devolve o folderId da pasta Sent, descoberto uma vez via API de folders.
     * Os emails enviados via sendEmail ficam na Sent — é onde temos de listar.
     */
    public synchronized String getSentFolderId() throws Exception {
        if (cachedSentFolderId == null) {
            var accountId = getAccountId();
            var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/folders";

            var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
            OAuthRequest request = new OAuthRequest(Verb.GET, url);
            request.addHeader("Accept", "application/json");
            service.signRequest(accessToken, request);

            try (Response response = service.execute(request)) {
                if (response.isSuccessful()) {
                    var reply = JSON.decode(response.getBody(), ZohoFolderListReply.class);
                    cachedSentFolderId = reply.data().stream()
                            .filter(f -> "Sent".equals(f.folderType()))
                            .map(ZohoFolder::folderId)
                            .findFirst()
                            .orElseThrow(() -> new Exception("Sent folder not found"));
                    Log.info("Zoho: Sent folderId = " + cachedSentFolderId);
                } else {
                    throw new Exception("getSentFolderId failed: " + response.getCode() + "/" + response.getBody());
                }
            }
        }
        return cachedSentFolderId;
    }

    /**
     * Envia um email para a própria conta Zoho.
     * O email fica na pasta Sent (standard Zoho behaviour).
     * Devolve o ZohoEmailItem com messageId — o folderId pode vir null
     * na resposta de envio, por isso usamos getSentFolderId() explicitamente.
     */
    public ZohoEmailItem sendEmail(String fromAddress, String toAddress,
                                   String subject, String content) throws Exception {
        var accountId = getAccountId();
        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages";

        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.POST, url);
        request.addHeader("Content-Type", "application/json");
        request.addHeader("Accept", "application/json");
        var payload = new ZohoSendEmailRequest(fromAddress, toAddress, subject, content);
        request.setPayload(JSON.encode(payload));
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var reply = JSON.decode(response.getBody(), ZohoSendEmailReply.class);
                var item = reply.data();
                // Se o folderId não vier na resposta de envio, forçamos o da pasta Sent
                if (item != null && (item.folderId() == null || item.folderId().isBlank())) {
                    var sentFolderId = getSentFolderId();
                    item = new ZohoEmailItem(item.messageId(), item.subject(), sentFolderId, item.receivedTime());
                }
                return item;
            } else {
                System.err.println("sendEmail failed: " + response.getCode() + "/" + response.getBody());
                return null;
            }
        }
    }

    /**
     * Lista emails da pasta Sent com o nosso SUBJECT_PREFIX.
     * É aqui que estão os emails que enviámos — não na Inbox.
     */
    public List<ZohoEmailItem> listSystemEmails() throws Exception {
        var accountId = getAccountId();
        var sentFolderId = getSentFolderId();

        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId
                + "/messages/view?folderId=" + sentFolderId
                + "&limit=" + PAGE_LIMIT + "&start=1";

        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, url);
        request.addHeader("Accept", "application/json");
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var reply = JSON.decode(response.getBody(), ZohoEmailListReply.class);
                if (reply.data() == null) return Collections.emptyList();
                // Cada item da lista da pasta Sent já tem o folderId correcto
                return reply.data().stream()
                        .filter(item -> item.subject() != null && item.subject().startsWith(SUBJECT_PREFIX))
                        .toList();
            } else {
                System.err.println("listSystemEmails failed: " + response.getCode() + "/" + response.getBody());
                return Collections.emptyList();
            }
        }
    }

    /**
     * Obtém o conteúdo HTML/text de um email.
     * O folderId deve ser o da pasta onde o email está (Sent).
     */
    public String getEmailContent(String folderId, String messageId) throws Exception {
        var accountId = getAccountId();
        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId
                + "/folders/" + folderId
                + "/messages/" + messageId
                + "/content";

        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, url);
        request.addHeader("Accept", "application/json");
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var reply = JSON.decode(response.getBody(), ZohoEmailContentReply.class);
                return reply.data() != null ? reply.data().content() : null;
            } else {
                System.err.println("getEmailContent failed: " + response.getCode() + "/" + response.getBody());
                return null;
            }
        }
    }

    /**
     * Apaga um email. O folderId tem de corresponder à pasta onde o email está.
     */
    public void deleteEmail(String folderId, String messageId) throws Exception {
        var accountId = getAccountId();
        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId
                + "/folders/" + folderId
                + "/messages/" + messageId;

        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.DELETE, url);
        request.addHeader("Accept", "application/json");
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (!response.isSuccessful())
                System.err.println("deleteEmail failed: " + response.getCode() + "/" + response.getBody());
        }
    }
}