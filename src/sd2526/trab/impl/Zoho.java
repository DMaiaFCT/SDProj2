package sd2526.trab.impl;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import sd2526.trab.impl.zoho.ZohoServiceFactory;
import sd2526.trab.impl.zoho.ZohoTokenManager;
import sd2526.trab.impl.zoho.msgs.ZohoAccount;
import sd2526.trab.impl.zoho.msgs.ZohoAccountReply;
import sd2526.trab.impl.utils.JSON;

public class Zoho {
    static final String MAIL_API_BASE = "https://mail.zoho.eu/api";

    static final String CLIENT_ID = "1000.OSD1FRO943P8LWDAGY57WAUUPJBG1Z";
    static final String CLIENT_SECRET = "013c57580b5583a9c6dd6b7e6a718bba8f7fc15fa7";
    static final String REFRESH_TOKEN = "1000.f7044213801b56912f8d12fe4ad67089.0aec49724235ceafb1414fed6a3b1f38";

    private static final String ACCOUNTS = "/accounts";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;

    static Zoho instance;

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
                System.err.println(response.getCode() + "/" + response.getBody());
                return null;
            }
        }
    }

    //depois fazer um getAccountId e outros metodos para endpoints relevantes, criar records para as respostas (as que necessitam de resposta)


}