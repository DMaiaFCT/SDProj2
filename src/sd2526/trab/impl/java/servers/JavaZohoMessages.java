package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.ErrorCode.*;
import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
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
import sd2526.trab.impl.utils.IP;

//em vez de hibernate, usar o zoho

public class JavaZohoMessages extends JavaBaseService implements Messages, AdminMessages {

    private static final int REMOTE_COMM_DEADLINE = 90000;
    private static final long MESSAGES_CACHE_EXPIRATION = 30000;

    final JobDispatcher jobs;
    final AtomicLong counter = new AtomicLong(0L);
    private static Logger Log = Logger.getLogger(JavaZohoMessages.class.getName());


    protected final Cache<String, Message> messagesCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(MESSAGES_CACHE_EXPIRATION))
            .build();

    private JavaZohoMessages() {
        this.jobs = new JobDispatcher();
    }

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info(() -> "postMessage : pwd = %s, msg = %s\n".formatted(pwd, msg));

        return getUser(msg.getSender(), pwd)
                .thenWith((user) -> doAsyncPost(user, msg));
    }

    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "getInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

        if (badParams(name, mid, pwd))
            return error(BAD_REQUEST);

        return getUser(name, pwd)
                .thenWith(user -> {
                    try {
                        var zohoId = Zoho.getInstance().findZohoMessageId(mid);
                        if (zohoId == null)
                            return error(NOT_FOUND);

                        var msg = Zoho.getInstance().getEmailContent(zohoId);
                        return msg != null ? ok(msg) : error(NOT_FOUND);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return error(INTERNAL_ERROR);
                    }
                });
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        Log.info(() -> "getAllInboxMessages : name = %s, pwd = %s\n".formatted(name, pwd));

        return getUser(name, pwd)
                .thenWith(user -> {
                    try {
                        var emails = Zoho.getInstance().listEmails();
                        var ids = emails.stream()
                                .filter(e -> e.subject() != null && e.subject().startsWith(Zoho.SUBJECT_PREFIX))
                                .map(e -> e.subject().substring(Zoho.SUBJECT_PREFIX.length()))
                                .toList();
                        return ok(ids);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return error(INTERNAL_ERROR);
                    }
                });
    }

    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        Log.info(() -> "searchInbox : name = %s, pwd = %s, query=%s\n".formatted(name, pwd, query));

        return getUser(name, pwd)
                .thenWith(user -> {
                    try {
                        var emails = Zoho.getInstance().listEmails();
                        var upperQuery = query.toUpperCase();
                        var results = new ArrayList<String>();

                        for (var item : emails) {
                            if (item.subject() == null || !item.subject().startsWith(Zoho.SUBJECT_PREFIX))
                                continue;

                            var msg = Zoho.getInstance().getEmailContent(item.messageId());
                            if (msg == null) continue;

                            var subjectMatch = msg.getSubject() != null && msg.getSubject().toUpperCase().contains(upperQuery);
                            var contentsMatch = msg.getContents() != null && msg.getContents().toUpperCase().contains(upperQuery);

                            if (subjectMatch || contentsMatch)
                                results.add(msg.getId());
                        }
                        return ok(results);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return error(INTERNAL_ERROR);
                    }
                });
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "removeInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

        return getUser(name, pwd)
                .thenWith(user -> {
                    try {
                        var zohoId = Zoho.getInstance().findZohoMessageId(mid);
                        if (zohoId == null)
                            return error(NOT_FOUND);

                        Zoho.getInstance().deleteEmail(zohoId);
                        return ok();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return error(INTERNAL_ERROR);
                    }
                });
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info(() -> "deleteMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));


        return getUser(name, pwd)
                .then(() -> getCachedMessage(mid))
                .thenWith(msg -> name.equals(getName(msg.senderAddress())) ? ok(msg) : error(FORBIDDEN))
                .thenWith(msg -> doAsyncDelete(msg));
    }


    protected Result<User> getUser(String user, String pwd) {
        try {
            var name = user.split("@", 2)[0];
            return Clients.UsersClient.get().getUser(name, pwd);
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        Log.info(() -> "postRemoteMessage : msg = %s\n".formatted(msg));

        try {
            Zoho.getInstance().sendMessage(msg);
            return ok();
        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        Log.info(() -> "remoteDeleteMessage : mid = %s\n".formatted(mid));

        try {
            var zohoId = Zoho.getInstance().findZohoMessageId(mid);
            if (zohoId != null)
                Zoho.getInstance().deleteEmail(zohoId);

            return ok();
        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    protected Result<Message> getCachedMessage(String mid) {
        var msg = messagesCache.getIfPresent(mid);
        return msg != null ? ok(msg) : error(FORBIDDEN);
    }

    public final class JobDispatcher {

        private final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<>();

        public void submit(String domain, Runnable job) {
            ExecutorService executor = executors.computeIfAbsent(
                    domain,
                    d -> Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r);
                        t.setUncaughtExceptionHandler((thr, ex) -> {
                            ex.printStackTrace();
                        });
                        return t;
                    })
            );
            executor.submit(job);
        }
    }

    public Result<String> doAsyncPost(User sender, Message msg) {

        // If we've seen this message before (same originId), return the existing ID.
        // This is the idempotency guard — repeated posts with the same message
        // object (same sender + creationTime) are silently de-duped.
        return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {

            msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));

            messagesCache.put(msg.originId(), new Message(msg)); // idempotency entry

            msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));

            messagesCache.put(msg.getId(), msg); // delete-within-30s entry

            var localAddresses = getLocalRecipientAddresses(msg);
            var remoteAddresses = getRemoteRecipientAddresses(msg);

            if (!localAddresses.isEmpty())
                postToZohoInbox(localAddresses, msg);

            if (!remoteAddresses.isEmpty()) {
                var remoteTargets = remoteAddresses.stream().collect(
                        Collectors.groupingBy(super::getDomain, Collectors.toSet()));

                for (var e : remoteTargets.entrySet()) {
                    var domain = e.getKey();
                    var domainRecipients = e.getValue();

                    jobs.submit(domain, () -> {
                        var res = super.reTry(
                                () -> Clients.AdminMessagesClient.get(domain).remotePostMessage(msg),
                                REMOTE_COMM_DEADLINE);

                        if (res.error() == ErrorCode.TIMEOUT)
                            for (var address : domainRecipients)
                                postToZohoInbox(Set.of(msg.senderAddress()), msg.cloneWithTimeout(address));
                    });
                }
            }
            return Result.ok(msg.getId());
        });
    }

    private void postToZohoInbox(Collection<String> addresses, Message msg) {
        try {
            var unknownResult = Clients.AdminUsersClient.get().checkUsers(addresses);

            var knownAddresses = new HashSet<>(addresses);
            if (unknownResult.isOK())
                knownAddresses.removeAll(unknownResult.value());

            if (!knownAddresses.isEmpty())
                Zoho.getInstance().sendMessage(msg);

            if (unknownResult.isOK() && !unknownResult.value().isEmpty())
                reportUnknownRecipients(unknownResult.value(), msg);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void reportUnknownRecipients(Collection<String> unknownAddresses, Message msg) {
        var senderDomain = super.getDomain(msg.senderAddress());

        for (var address : unknownAddresses) {
            var errorMsg = msg.cloneWithUserNotFound(address);
            if (super.isLocalDomain(senderDomain)) {
                try {
                    Zoho.getInstance().sendMessage(errorMsg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                doAsyncRemotePost(senderDomain, errorMsg);
            }
        }
    }

    public Result<Void> doAsyncDelete(Message msg) {
        var domains = msg.getDestination().stream().map(r -> r.split("@")[1]).collect(Collectors.toSet());
        for (var domain : domains) {
            if (domain.equals(IP.domain())) {
                try {
                    var zohoId = Zoho.getInstance().findZohoMessageId(msg.getId());
                    if (zohoId != null)
                        Zoho.getInstance().deleteEmail(zohoId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                jobs.submit(domain, () ->
                        super.reTry(() -> Clients.AdminMessagesClient.get(domain)
                                .remoteDeleteMessage(msg.getId()), REMOTE_COMM_DEADLINE));
            }
        }
        return ok();
    }

    public void doAsyncRemotePost(String remoteDomain, Message msg) {
        Log.info(() -> "\nenqueueRemotePost : remoteDomain=%s, msg = %s\n".formatted(remoteDomain, msg));
        jobs.submit(remoteDomain, () -> {
            super.reTry(() -> Clients.AdminMessagesClient.get(remoteDomain).remotePostMessage(msg), REMOTE_COMM_DEADLINE);
        });
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        Log.info(() -> "remoteDeleteUserInbox : name = %s\n".formatted(name));

        try {
            var emails = Zoho.getInstance().listEmails();
            for (var item : emails)
                Zoho.getInstance().deleteEmail(item.messageId());
            return ok();
        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }

    }


    private List<String> getLocalRecipientAddresses(Message msg) {
        return msg.getDestination().stream().filter(super::isLocalAddress).toList();
    }

    private Set<String> getRemoteRecipientAddresses(Message msg) {
        return msg.getDestination().stream().filter(Predicate.not(super::isLocalAddress)).collect(Collectors.toSet());
    }


    static JavaZohoMessages instance;

    public static synchronized JavaZohoMessages getInstance() {
        if (instance == null)
            instance = new JavaZohoMessages();
        return instance;
    }
}

