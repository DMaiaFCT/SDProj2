package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.SyncPoint;
import sd2526.trab.impl.rest.servers.VersionHeaderHandler;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.Sleep;
import sd2526.trab.impl.utils.KafkaPublisher;
import sd2526.trab.impl.utils.KafkaSubscriber;
import sd2526.trab.impl.utils.KafkaUtils;
import sd2526.trab.impl.utils.RecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.google.gson.Gson;

public class JavaMessages extends JavaBaseService implements Messages, AdminMessages {

    private final ConcurrentHashMap<String, Long> largestSeenRemoteSid = new ConcurrentHashMap<>();

    private final Gson gson = new Gson();
    private KafkaPublisher publisher;
    private KafkaSubscriber subscriber;
    private final String myKafkaTopic = "messages-topic-" + THIS_DOMAIN;

    private static final int REMOTE_COMM_DEADLINE = 90000;
    private static final long MESSAGES_CACHE_EXPIRATION = 30000;
    private static final long DIRTY_INBOX_CACHE_EXPIRATION = 10000;

    final JobDispatcher jobs;
    final AtomicLong counter = new AtomicLong(0L);
    private static Logger Log = Logger.getLogger(JavaMessages.class.getName());

    protected final Cache<String, Message> messagesCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(MESSAGES_CACHE_EXPIRATION))
            .build();

    protected final Cache<String, String> gcDeletedMessageCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(DIRTY_INBOX_CACHE_EXPIRATION))
            .removalListener( (removed) -> {

                var sqlExpr = """
                   SELECT * FROM Message m
                      WHERE NOT EXISTS 
                         (SELECT 1 FROM InboxEntry e WHERE e.mid = m.id)
                   """;

                DB.transaction( (hibernate) ->
                        hibernate.select( sqlExpr, Message.class )
                                .thenWith( (orphans) -> hibernate.deleteMany( orphans ))
                );
            })
            .build();

    private JavaMessages() {
        this.jobs = new JobDispatcher();
        this.initializeKafkaReplication();
    }

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info( () -> "postMessage via Kafka: pwd = %s, msg = %s\n".formatted(pwd, msg));

        if (msg.getId() != null && messagesCache.getIfPresent(msg.getId()) != null) {
            return ok(msg.getId());
        }
        if (msg.originId() != null && messagesCache.getIfPresent(msg.originId()) != null) {
            return ok(messagesCache.getIfPresent(msg.originId()).getId());
        }

        var authResult = getUser(msg.getSender(), pwd);
        if (!authResult.isOK()) {
            return error(authResult.error());
        }

        User sender = authResult.value();
        msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));

        if (msg.getId() != null && messagesCache.getIfPresent(msg.getId()) != null) {
            return ok(msg.getId());
        }
        if (msg.originId() != null && messagesCache.getIfPresent(msg.originId()) != null) {
            return ok(messagesCache.getIfPresent(msg.originId()).getId());
        }

        String jsonPayload = gson.toJson(msg);

        long assignedOffset = publisher.publish(myKafkaTopic, jsonPayload);
        if (assignedOffset < 0) {
            return error(INTERNAL_ERROR);
        }

        String generatedMessageId = SyncPoint.getSyncPoint().waitForResult(assignedOffset);

        VersionHeaderHandler.version.set(assignedOffset);

        return ok(generatedMessageId);
    }

    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        if( badParams( name, mid, pwd ) ) return error(BAD_REQUEST);

        Long clientReadThreshold = VersionHeaderHandler.version.get();
        if (clientReadThreshold != null && clientReadThreshold >= 0) {
            SyncPoint.getSyncPoint().waitForVersion(clientReadThreshold);
        }

        return getUser(name, pwd)
                .then( () -> DB.getOne( new InboxEntry(mid, name), InboxEntry.class))
                .then(() -> DB.getOne( mid, Message.class));
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        Long clientReadThreshold = VersionHeaderHandler.version.get();
        if (clientReadThreshold != null && clientReadThreshold >= 0) {
            SyncPoint.getSyncPoint().waitForVersion(clientReadThreshold);
        }

        var sqlExpr = "SELECT m.mid FROM InboxEntry m WHERE m.recipient = '%s'".formatted(name);
        return getUser(name, pwd ).then( () -> DB.select( sqlExpr, String.class));
    }

    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        Log.info( () -> "searchInbox : name = %s, pwd = %s, query=%s\n".formatted(name, pwd, query));

        Long clientReadThreshold = VersionHeaderHandler.version.get();
        if (clientReadThreshold != null && clientReadThreshold >= 0) {
            SyncPoint.getSyncPoint().waitForVersion(clientReadThreshold);
        }

        // resolve problema no teste 110d. normalmente "'" é só um delimiter, substituir por "''" arranja isso.
        String sanitizedQuery = query.replace("'", "''").toUpperCase();

        var sqlExpr = """
             SELECT m.id FROM Message m
             WHERE EXISTS (
                SELECT 1 FROM InboxEntry e WHERE e.mid = m.id AND e.recipient = '%s'
             )
             AND (upper(m.subject) LIKE '%%%s%%' OR upper(m.contents) LIKE '%%%s%%')
             """.formatted(name, sanitizedQuery, sanitizedQuery);

        return getUser(name, pwd )
                .then( () -> DB.select( sqlExpr, String.class));
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info( () -> "removeInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

        var auth = getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        Message removeToken = new Message();
        removeToken.setSubject("REMOVE");
        removeToken.setId(mid);
        removeToken.setSender(name);

        long assignedOffset = publisher.publish(myKafkaTopic, gson.toJson(removeToken));
        if (assignedOffset < 0) return error(INTERNAL_ERROR);

        SyncPoint.getSyncPoint().waitForResult(assignedOffset);
        VersionHeaderHandler.version.set(assignedOffset);

        return ok();
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info( () -> "deleteMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

        var auth = getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        var msgResult = getCachedMessage(mid);
        if (!msgResult.isOK()) return error(msgResult.error());
        Message msg = msgResult.value();

        if (!name.equals(getName(msg.senderAddress()))) return error(FORBIDDEN);

        Message deleteToken = new Message();
        deleteToken.setSubject("DELETE");
        deleteToken.setId(mid);
        deleteToken.setSender(msg.getSender());
        deleteToken.setDestination(msg.getDestination());

        long assignedOffset = publisher.publish(myKafkaTopic, gson.toJson(deleteToken));
        if (assignedOffset < 0) return error(INTERNAL_ERROR);

        SyncPoint.getSyncPoint().waitForResult(assignedOffset);
        VersionHeaderHandler.version.set(assignedOffset);

        return ok();
    }

    protected Result<User> getUser( String user, String pwd) {
        try {
            var name = user.split("@", 2)[0];
            return Clients.UsersClient.get().getUser( name, pwd);
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    protected Result<Set<String>> checkUsers( Collection<String> addresses ) {
        return Clients.AdminUsersClient.get().checkUsers(addresses);
    }

    private void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg) {
        Log.info( () -> "deliverToKnownLocalRecipients : local known addresses = %s, msg = %s\n".formatted(addresses, msg));

        DB.transaction((hibernate) -> {
            hibernate.persistOne( msg );
            for( var address : addresses )
                hibernate.persistOne( new InboxEntry( msg.getId(), getName(address) ));

            return ok();
        });
    }

    private void reportUnknownLocalRecipients(Collection<String> addresses, Message msg) {
        Log.info( () -> "reportUnknownLocalRecipients : unknown addresses = %s, msg = %s\n".formatted(addresses, msg));

        var senderDomain = super.getDomain( msg.senderAddress() );

        try {
            for( var recipientAddress : addresses ) {
                var errorMsg = msg.cloneWithUserNotFound( recipientAddress );
                if( super.isLocalDomain( senderDomain ) ) {
                    DB.transaction((hibernate)-> {
                        hibernate.persistOne( new InboxEntry( errorMsg.getId(), msg.senderName() )) ;
                        hibernate.persistOne(errorMsg);
                        return ok();
                    });
                }
                else doAsyncRemotePost(senderDomain, errorMsg);
            }
        } catch( Exception x ) {
            x.printStackTrace();
        }
    }

    private Result<Void> postToLocalInboxes( Collection<String> addresses, Message msg) {
        Log.info( () -> "postToLocalInboxes : localRecipients = %s, msg = %s\n".formatted(addresses, msg));

        return checkUsers(addresses)
                .thenWith( unknownAddresses -> {

                    var knownAddresses = new HashSet<>( addresses );
                    knownAddresses.removeAll( unknownAddresses );

                    if( knownAddresses.size() > 0 )
                        deliverToKnownLocalRecipients(knownAddresses, msg);

                    if( unknownAddresses.size() > 0 )
                        reportUnknownLocalRecipients( unknownAddresses, msg );

                    return ok();
                });
    }

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        Log.info( () -> "postRemoteMessage : msg = %s\n".formatted(msg));

        var localAddresses = getLocalRecipientAddresses(msg);
        return postToLocalInboxes(localAddresses, msg);
    }

    private Result<Void> deleteFromLocalInbox(String mid) {
        Log.info( () -> "deleteFromLocalInbox : mid = %s\n".formatted(mid));

        var sql = "SELECT * FROM InboxEntry e WHERE e.mid = '%s'".formatted(mid);

        return DB.transaction( hibernate -> {

            hibernate.getOne(mid, Message.class)
                    .thenWith(msg -> hibernate.deleteOne(msg));

            return hibernate.select(sql, InboxEntry.class)
                    .thenWith( (entries) -> hibernate.deleteMany( entries ) );
        });
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        Log.info( () -> "remoteDeleteMessage : mid = %s\n".formatted(mid));

        return deleteFromLocalInbox(mid);
    }

    protected Result<Message> getCachedMessage( String mid ) {
        var msg = messagesCache.getIfPresent( mid );
        return msg != null ? ok( msg ) : error( FORBIDDEN );
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
            executor.submit( job );
        }
    }

    public Result<String> doAsyncPost(User sender, Message msg) {

        return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {

            msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));

            messagesCache.put(msg.originId(), new Message( msg ));

            msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));

            messagesCache.put(msg.getId(), msg);

            var localAdresses = getLocalRecipientAddresses(msg);
            var remoteAddresses = getRemoteRecipientAddresses(msg);

            System.out.println("Local Recipients:" + localAdresses);
            System.out.println("Remote Recipients:" + remoteAddresses);

            if (localAdresses.size() > 0)
                postToLocalInboxes(localAdresses, msg);

            if (remoteAddresses.size() > 0) {

                var remoteTargets = remoteAddresses.stream().collect(
                        Collectors.groupingBy( super::getDomain, Collectors.mapping( address -> address, Collectors.toSet())));

                for (var e : remoteTargets.entrySet()) {
                    var domain = e.getKey();
                    var domainRecipientAddressess = e.getValue();

                    jobs.submit(domain, () -> {
                        var res = super.reTry(() -> Clients.AdminMessagesClient.get(domain).remotePostMessage(msg), REMOTE_COMM_DEADLINE);
                        if (res.error() == ErrorCode.TIMEOUT) {
                            for (var address : domainRecipientAddressess)
                                postToLocalInboxes(Set.of(msg.senderAddress()), msg.cloneWithTimeout(address));
                        }
                    });
                }
            }
            return Result.ok(msg.getId());
        });
    }

    public Result<Void> doAsyncDelete( Message msg ) {
        var domains = msg.getDestination().stream().map( r -> r.split("@")[1]).collect( Collectors.toSet() );
        for( var domain : domains )
            if( domain.equals( IP.domain() ))
                deleteFromLocalInbox( msg.getId() );
            else
                jobs.submit(domain, () -> {
                    super.reTry(()-> Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(msg.getId()), REMOTE_COMM_DEADLINE);
                });
        return Result.ok();
    }

    public void doAsyncRemotePost( String remoteDomain, Message msg ) {
        Log.info( () -> "\nenqueueRemotePost : remoteDomain=%s, msg = %s\n".formatted(remoteDomain, msg));
        jobs.submit(remoteDomain, () -> {
            super.reTry(() -> Clients.AdminMessagesClient.get(remoteDomain).remotePostMessage(msg), REMOTE_COMM_DEADLINE);
        });
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        Log.info( () -> "remoteDeleteUserInbox : name = %s\n".formatted(name));

        var sqlExpr = "SELECT * FROM InboxEntry e WHERE e.recipient = '%s'".formatted(name);

        return DB.transaction( hibernate -> {

            return hibernate.select(sqlExpr, InboxEntry.class)
                    .thenWith( (entries) -> {
                        hibernate.deleteMany( entries );
                        for( var e: entries )
                            gcDeletedMessageCache.put( e.mid, e.mid);

                        return ok();
                    } );
        });
    }

    private List<String> getLocalRecipientAddresses(  Message msg ) {
        return msg.getDestination().stream().filter( super::isLocalAddress ).toList();
    }

    private Set<String> getRemoteRecipientAddresses(  Message msg ) {
        return msg.getDestination().stream().filter( Predicate.not(super::isLocalAddress)).collect( Collectors.toSet() );
    }

    static JavaMessages instance;

    public static synchronized JavaMessages getInstance() {
        if( instance == null )
            instance = new JavaMessages();
        return instance;
    }

    public void initializeKafkaReplication() {
        String kafkaBrokers = "kafka:9092,localhost:9092";

        this.publisher = KafkaPublisher.createPublisher(kafkaBrokers);

        new Thread(() -> {
            try {
                KafkaUtils.createTopic(myKafkaTopic);
            } catch (Exception e) {
                Log.warning("Topic creation deferred: " + e.getMessage());
            }
        }).start();

        this.subscriber = KafkaSubscriber.createSubscriber(kafkaBrokers, List.of(myKafkaTopic));
        this.subscriber.start(new RecordProcessor() {
            @Override
            public void onReceive(ConsumerRecord<String, String> record) {
                processIncomingKafkaEvent(record);
            }
        });
        Log.info(() -> "Kafka replication worker engine initialized on topic: " + myKafkaTopic);
    }

    private void processIncomingKafkaEvent(ConsumerRecord<String, String> record) {
        long currentOffset = record.offset();
        Message committedMsg = gson.fromJson(record.value(), Message.class);

        if ("DELETE".equals(committedMsg.getSubject())) {
            deleteFromLocalInbox(committedMsg.getId());

            if (super.isLocalAddress(committedMsg.getSender())) {
                var domains = committedMsg.getDestination().stream().map(r -> r.split("@")[1]).collect(Collectors.toSet());
                for (var domain : domains) {
                    if (!domain.equals(IP.domain())) {
                        jobs.submit(domain, () -> {
                            super.reTry(() -> Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(committedMsg.getId()), REMOTE_COMM_DEADLINE);
                        });
                    }
                }
            }
            VersionHeaderHandler.version.set(currentOffset);
            SyncPoint.getSyncPoint().setResult(currentOffset, committedMsg.getId());
            return;
        }

        if ("REMOVE".equals(committedMsg.getSubject())) {
            DB.transaction(hibernate -> hibernate.deleteOne(new InboxEntry(committedMsg.getId(), committedMsg.getSender())).mapToVoid());
            gcDeletedMessageCache.put(committedMsg.getId(), committedMsg.getId());
            VersionHeaderHandler.version.set(currentOffset);
            SyncPoint.getSyncPoint().setResult(currentOffset, committedMsg.getId());
            return;
        }

        String senderDomain = super.getDomain(committedMsg.senderAddress());

        if (!super.isLocalDomain(senderDomain)) {
            try {
                String[] idParts = committedMsg.getId().split("\\+");
                if (idParts.length == 2) {
                    long incomingSid = Long.parseLong(idParts[1]);
                    long maximumProcessedSid = largestSeenRemoteSid.getOrDefault(senderDomain, -1L);
                    if (incomingSid <= maximumProcessedSid) {
                        SyncPoint.getSyncPoint().setResult(currentOffset, committedMsg.getId());
                        return;
                    }
                    largestSeenRemoteSid.put(senderDomain, incomingSid);
                }
            } catch (Exception e) {
                Log.warning("Could not parse replication sequence identifier from message ID: " + committedMsg.getId());
            }
        }

        if (super.isLocalDomain(senderDomain)) {
            committedMsg.setId("%s+%04d".formatted(THIS_DOMAIN, currentOffset));
        }
        messagesCache.put(committedMsg.getId(), committedMsg);

        var localAddresses = getLocalRecipientAddresses(committedMsg);
        if (localAddresses.size() > 0) {
            postToLocalInboxes(localAddresses, committedMsg);
        }

        var remoteAddresses = getRemoteRecipientAddresses(committedMsg);
        if (remoteAddresses.size() > 0 && super.isLocalAddress(committedMsg.senderAddress())) {

            var remoteTargets = remoteAddresses.stream().collect(
                    Collectors.groupingBy(super::getDomain, Collectors.mapping(address -> address, Collectors.toSet())));

            for (var e : remoteTargets.entrySet()) {
                var domain = e.getKey();
                var domainRecipientAddresses = e.getValue();

                jobs.submit(domain, () -> {
                    var res = super.reTry(() -> Clients.AdminMessagesClient.get(domain).remotePostMessage(committedMsg), REMOTE_COMM_DEADLINE);
                    if (res.error() == ErrorCode.TIMEOUT) {
                        for (var address : domainRecipientAddresses)
                            postToLocalInboxes(Set.of(committedMsg.senderAddress()), committedMsg.cloneWithTimeout(address));
                    }
                });
            }
        }

        VersionHeaderHandler.version.set(currentOffset);
        SyncPoint.getSyncPoint().setResult(currentOffset, committedMsg.getId());
    }
}