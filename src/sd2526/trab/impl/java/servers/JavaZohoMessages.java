package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.ErrorCode.*;
import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.Zoho;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.zoho.msgs.ZohoEmailItem;
import sd2526.trab.impl.utils.JSON;
import sd2526.trab.impl.utils.IP;

public class JavaZohoMessages extends JavaBaseService implements Messages, AdminMessages {

    private static final int REMOTE_COMM_DEADLINE = 90000;
    private static final Logger Log = Logger.getLogger(JavaZohoMessages.class.getName());

    final JobDispatcher jobs;
    final AtomicLong counter = new AtomicLong(0L);

    // mid do sistema → Message (fonte de verdade para leituras)
    private final ConcurrentHashMap<String, Message> inbox = new ConcurrentHashMap<>();

    // mid do sistema → ZohoEmailItem (tem messageId e folderId para delete no Zoho)
    private final ConcurrentHashMap<String, ZohoEmailItem> zohoIndex = new ConcurrentHashMap<>();

    // IDs já entregues via remotePostMessage — evita guardar duplicados
    private final Set<String> deliveredIds = ConcurrentHashMap.newKeySet();

    // Cache de 30s usada por deleteMessage para saber os domínios destino da mensagem
    private final Cache<String, Message> recentMessages = CacheBuilder.newBuilder()
            .expireAfterWrite(java.time.Duration.ofMillis(30000))
            .build();

    private JavaZohoMessages(boolean cleanState) {
        this.jobs = new JobDispatcher();
        if (cleanState)
            purgeZoho();
        else
            rebuildFromZoho();
    }

    // -------------------------------------------------------------------
    // Arranque
    // -------------------------------------------------------------------

    /**
     * Apaga todos os emails do sistema no Zoho. Chamado no primeiro arranque.
     */
    private void purgeZoho() {
        try {
            Log.info("Zoho: arranque limpo — a apagar emails antigos...");
            var emails = Zoho.getInstance().listSystemEmails();
            for (var item : emails) {
                try {
                    Zoho.getInstance().deleteEmail(item.folderId(), item.messageId());
                } catch (Exception e) {
                    Log.warning("Zoho: falha ao apagar " + item.messageId() + ": " + e.getMessage());
                }
            }
            Log.info("Zoho: " + emails.size() + " emails apagados. Pronto.");
        } catch (Exception e) {
            Log.warning("Zoho: purge falhou (não fatal): " + e.getMessage());
        }
    }

    /**
     * Reconstrói a cache a partir dos emails guardados no Zoho.
     * Chamado quando o servidor reinicia com estado guardado (cleanState=false).
     * <p>
     * Cada email tem no body o JSON da mensagem (ver storeMessage).
     * O Zoho pode envolver o body em HTML, por isso fazemos strip antes de parsear.
     */
    private void rebuildFromZoho() {
        try {
            Log.info("Zoho: restart — a reconstruir estado...");
            var emails = Zoho.getInstance().listSystemEmails();
            long maxCounter = 0L;

            for (var item : emails) {
                try {
                    var rawContent = Zoho.getInstance().getEmailContent(item.folderId(), item.messageId());
                    if (rawContent == null) continue;

                    var msg = JSON.decode(stripHtml(rawContent), Message.class);
                    if (msg == null || msg.getId() == null) continue;

                    inbox.put(msg.getId(), msg);
                    zohoIndex.put(msg.getId(), item);
                    deliveredIds.add(msg.getId());

                    maxCounter = Math.max(maxCounter, extractCounter(msg.getId()));
                } catch (Exception e) {
                    Log.warning("Zoho: falha ao recuperar email " + item.messageId() + ": " + e.getMessage());
                }
            }

            counter.set(maxCounter);
            Log.info("Zoho: " + inbox.size() + " mensagens recuperadas. Pronto.");
        } catch (Exception e) {
            Log.warning("Zoho: rebuild falhou (não fatal): " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------
    // Messages API
    // -------------------------------------------------------------------

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info(() -> "postMessage : pwd = %s, msg = %s".formatted(pwd, msg));
        return getUser(msg.getSender(), pwd)
                .thenWith(user -> doAsyncPost(user, msg));
    }

    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "getInboxMessage : name = %s, mid = %s".formatted(name, mid));
        if (badParams(name, mid, pwd)) return error(BAD_REQUEST);
        return getUser(name, pwd).thenWith(user -> {
            var msg = inbox.get(mid);
            return msg != null ? ok(msg) : error(NOT_FOUND);
        });
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        Log.info(() -> "getAllInboxMessages : name = %s".formatted(name));
        return getUser(name, pwd)
                .thenWith(user -> ok(new ArrayList<>(inbox.keySet())));
    }

    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        Log.info(() -> "searchInbox : name = %s, query = %s".formatted(name, query));
        if (badParams(name, pwd, query)) return error(BAD_REQUEST);
        return getUser(name, pwd).thenWith(user -> {
            var q = query.toUpperCase();
            var hits = inbox.values().stream()
                    .filter(m -> contains(m.getSubject(), q) || contains(m.getContents(), q))
                    .map(Message::getId)
                    .toList();
            return ok(hits);
        });
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "removeInboxMessage : name = %s, mid = %s".formatted(name, mid));
        if (badParams(name, mid, pwd)) return error(BAD_REQUEST);
        return getUser(name, pwd).thenWith(user -> {
            if (!inbox.containsKey(mid)) return error(NOT_FOUND);
            deleteMessage(mid);
            return ok();
        });
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info(() -> "deleteMessage : name = %s, mid = %s".formatted(name, mid));
        return getUser(name, pwd)
                .then(() -> {
                    var msg = recentMessages.getIfPresent(mid);
                    return msg != null ? ok(msg) : error(FORBIDDEN);
                })
                .thenWith(msg -> name.equals(getName(msg.senderAddress())) ? ok(msg) : error(FORBIDDEN))
                .thenWith(this::propagateDelete);
    }

    // -------------------------------------------------------------------
    // AdminMessages API (chamado por outros domínios)
    // -------------------------------------------------------------------

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        Log.info(() -> "remotePostMessage : msg = %s".formatted(msg));
        // Se já entregámos esta mensagem, ignoramos (idempotência para retries)
        if (!deliveredIds.add(msg.getId()))
            return ok();
        try {
            storeMessage(msg);
            return ok();
        } catch (Exception e) {
            deliveredIds.remove(msg.getId()); // permite retry legítimo
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        Log.info(() -> "remoteDeleteMessage : mid = %s".formatted(mid));
        deliveredIds.remove(mid);
        deleteMessage(mid);
        return ok();
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        Log.info(() -> "remoteDeleteUserInbox : name = %s".formatted(name));
        new ArrayList<>(inbox.keySet()).forEach(this::deleteMessage);
        deliveredIds.clear();
        return ok();
    }

    // -------------------------------------------------------------------
    // Lógica de routing de mensagens (local e remoto)
    // -------------------------------------------------------------------

    public Result<String> doAsyncPost(User sender, Message msg) {
        // Se já processámos esta mensagem (mesmo originId), devolvemos o ID existente
        var cached = recentMessages.getIfPresent(msg.originId());
        if (cached != null) return ok(cached.getId());

        msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));
        recentMessages.put(msg.originId(), new Message(msg)); // guard de idempotência

        msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));
        recentMessages.put(msg.getId(), msg); // guard para deleteMessage nos próximos 30s

        var local = localRecipients(msg);
        var remote = remoteRecipients(msg);
        Log.info(() -> "Local: %s | Remote: %s".formatted(local, remote));

        if (!local.isEmpty())
            deliverToLocal(local, msg);

        for (var entry : groupByDomain(remote).entrySet()) {
            var domain = entry.getKey();
            var recipients = entry.getValue();
            jobs.submit(domain, () -> {
                var res = super.reTry(() -> Clients.AdminMessagesClient.get(domain).remotePostMessage(msg), REMOTE_COMM_DEADLINE);
                if (res.error() == ErrorCode.TIMEOUT)
                    recipients.forEach(addr -> deliverToLocal(Set.of(msg.senderAddress()), msg.cloneWithTimeout(addr)));
            });
        }
        return ok(msg.getId());
    }

    /**
     * Verifica quais os destinatários existem e entrega; gera erro para os que não existem.
     */
    private void deliverToLocal(Collection<String> addresses, Message msg) {
        try {
            var unknownResult = Clients.AdminUsersClient.get().checkUsers(addresses);
            var known = new HashSet<>(addresses);
            if (unknownResult.isOK()) known.removeAll(unknownResult.value());

            if (!known.isEmpty())
                storeMessage(msg);

            if (unknownResult.isOK() && !unknownResult.value().isEmpty())
                notifyUnknownRecipients(unknownResult.value(), msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Para cada destinatário desconhecido, envia uma mensagem de erro ao remetente.
     */
    private void notifyUnknownRecipients(Collection<String> unknown, Message msg) {
        var senderDomain = getDomain(msg.senderAddress());
        for (var addr : unknown) {
            var errorMsg = msg.cloneWithUserNotFound(addr);
            if (isLocalDomain(senderDomain)) {
                try {
                    storeMessage(errorMsg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                jobs.submit(senderDomain, () ->
                        super.reTry(() -> Clients.AdminMessagesClient.get(senderDomain).remotePostMessage(errorMsg), REMOTE_COMM_DEADLINE));
            }
        }
    }

    /**
     * Apaga a mensagem do domínio local e pede a todos os domínios remotos para apagarem também.
     */
    public Result<Void> propagateDelete(Message msg) {
        for (var domain : domainsOf(msg)) {
            if (domain.equals(IP.domain()))
                deleteMessage(msg.getId());
            else
                jobs.submit(domain, () ->
                        super.reTry(() -> Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(msg.getId()), REMOTE_COMM_DEADLINE));
        }
        return ok();
    }

    // -------------------------------------------------------------------
    // Storage (Zoho + cache em memória)
    // -------------------------------------------------------------------

    /**
     * Guarda a mensagem no Zoho e na cache em memória.
     * O body do email é o JSON da mensagem — permite reconstruir o estado no restart.
     * Idempotente: se o mid já existe na cache, não duplica.
     */
    private void storeMessage(Message msg) throws Exception {
        if (inbox.containsKey(msg.getId())) return;

        var account = Zoho.getInstance().getAccount();
        if (account == null) throw new Exception("Não foi possível obter a conta Zoho");

        var zohoItem = Zoho.getInstance().sendEmail(
                account.primaryEmailAddress(),
                account.primaryEmailAddress(),
                Zoho.SUBJECT_PREFIX + msg.getId(),
                JSON.encode(msg)
        );

        inbox.put(msg.getId(), new Message(msg));
        if (zohoItem != null)
            zohoIndex.put(msg.getId(), zohoItem);
    }

    /**
     * Apaga a mensagem da cache em memória e do Zoho.
     */
    private void deleteMessage(String mid) {
        inbox.remove(mid);
        var zohoItem = zohoIndex.remove(mid);
        if (zohoItem == null) return;
        try {
            Zoho.getInstance().deleteEmail(zohoItem.folderId(), zohoItem.messageId());
        } catch (Exception e) {
            Log.warning("Falha ao apagar email do Zoho (mid=" + mid + "): " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------
    // Utilitários
    // -------------------------------------------------------------------

    protected Result<User> getUser(String user, String pwd) {
        try {
            return Clients.UsersClient.get().getUser(user.split("@", 2)[0], pwd);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    private List<String> localRecipients(Message msg) {
        return msg.getDestination().stream().filter(this::isLocalAddress).toList();
    }

    private Set<String> remoteRecipients(Message msg) {
        return msg.getDestination().stream().filter(a -> !isLocalAddress(a)).collect(Collectors.toSet());
    }

    private Map<String, Set<String>> groupByDomain(Set<String> addresses) {
        return addresses.stream().collect(Collectors.groupingBy(this::getDomain, Collectors.toSet()));
    }

    private Set<String> domainsOf(Message msg) {
        return msg.getDestination().stream().map(r -> r.split("@")[1]).collect(Collectors.toSet());
    }

    /**
     * Verifica se o campo de texto contém a query (case-insensitive).
     */
    private static boolean contains(String field, String upperQuery) {
        return field != null && field.toUpperCase().contains(upperQuery);
    }

    /**
     * Remove tags HTML do conteúdo devolvido pelo Zoho e decode entidades básicas.
     */
    private static String stripHtml(String content) {
        if (content == null) return "";
        return content
                .replaceAll("<[^>]+>", "")
                .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
                .replace("&quot;", "\"").replace("&apos;", "'").replace("&nbsp;", " ")
                .trim();
    }

    /**
     * Extrai o número sequencial do final de um ID como "ourorg0+0042" → 42.
     */
    private static long extractCounter(String mid) {
        int plus = mid.lastIndexOf('+');
        if (plus < 0) return 0L;
        try {
            return Long.parseLong(mid.substring(plus + 1));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    // -------------------------------------------------------------------
    // JobDispatcher — garante que requests para o mesmo domínio são seriais
    // -------------------------------------------------------------------

    public final class JobDispatcher {
        private final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<>();

        public void submit(String domain, Runnable job) {
            executors.computeIfAbsent(domain, d ->
                    Executors.newSingleThreadExecutor(r -> {
                        var t = new Thread(r);
                        t.setUncaughtExceptionHandler((thread, ex) -> ex.printStackTrace());
                        return t;
                    })
            ).submit(job);
        }
    }

    // -------------------------------------------------------------------
    // Singleton
    // -------------------------------------------------------------------

    private static volatile JavaZohoMessages instance;

    /**
     * Chamado pelo RestZohoServer.main() antes de o servidor arrancar.
     */
    public static synchronized JavaZohoMessages init(boolean cleanState) {
        if (instance == null)
            instance = new JavaZohoMessages(cleanState);
        return instance;
    }

    public static synchronized JavaZohoMessages getInstance() {
        if (instance == null)
            instance = new JavaZohoMessages(true);
        return instance;
    }
}