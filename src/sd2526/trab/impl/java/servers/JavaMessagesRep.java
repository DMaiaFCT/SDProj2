package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.google.gson.Gson;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.rest.servers.VersionHeaderHandler;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.KafkaPublisher;
import sd2526.trab.impl.utils.KafkaSubscriber;
import sd2526.trab.impl.utils.KafkaUtils;
import sd2526.trab.impl.utils.RecordProcessor;


public class JavaMessagesRep extends JavaMessages {

    private static final Logger Log = Logger.getLogger(JavaMessagesRep.class.getName());

    // O Kafka corre no container "kafka" na rede Docker do tester
    private static final String KAFKA_BROKERS = "kafka:9092";

    private final Gson gson = new Gson();
    private final KafkaPublisher publisher;
    private final String myKafkaTopic;

    // Set of remote message IDs already processed - used for deduplication
    private final ConcurrentHashMap<String, Boolean> seenRemoteMessageIds = new ConcurrentHashMap<>();

    private JavaMessagesRep() {
        myKafkaTopic = "messages-topic-" + THIS_DOMAIN;

        Log.info("JavaMessagesRep: a inicializar Kafka no tópico " + myKafkaTopic);

        KafkaUtils.createTopic(myKafkaTopic);

        this.publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);

        KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber(
                KAFKA_BROKERS, List.of(myKafkaTopic));
        subscriber.start(new RecordProcessor() {
            @Override
            public void onReceive(ConsumerRecord<String, String> record) {
                processKafkaEvent(record);
            }
        });

        Log.info("JavaMessagesRep pronto — Kafka topic: " + myKafkaTopic);
    }

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info(() -> "postMessage (rep): pwd=%s, msg=%s\n".formatted(pwd, msg));

        // Idempotência
        if (msg.getId() != null && messagesCache.getIfPresent(msg.getId()) != null)
            return ok(msg.getId());
        if (msg.originId() != null && messagesCache.getIfPresent(msg.originId()) != null)
            return ok(messagesCache.getIfPresent(msg.originId()).getId());

        var authResult = getUser(msg.getSender(), pwd);
        if (!authResult.isOK()) return error(authResult.error());

        var sender = authResult.value();
        msg.setSender("%s <%s@%s>".formatted(
                sender.getDisplayName(), sender.getName(), sender.getDomain()));

        long offset = publisher.publish(myKafkaTopic, gson.toJson(msg));
        if (offset < 0) return error(INTERNAL_ERROR);

        String mid = SyncPoint.getSyncPoint().waitForResult(offset);
        VersionHeaderHandler.version.set(offset);
        return ok(mid);
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "removeInboxMessage (rep): name=%s, mid=%s\n".formatted(name, mid));

        var auth = getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        Message token = new Message();
        token.setSubject("REMOVE");
        token.setId(mid);
        token.setSender(name);

        long offset = publisher.publish(myKafkaTopic, gson.toJson(token));
        if (offset < 0) return error(INTERNAL_ERROR);

        SyncPoint.getSyncPoint().waitForResult(offset);
        VersionHeaderHandler.version.set(offset);
        return ok();
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info(() -> "deleteMessage (rep): name=%s, mid=%s\n".formatted(name, mid));

        var auth = getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        var msgResult = getCachedMessage(mid);
        if (!msgResult.isOK()) return error(msgResult.error());
        Message msg = msgResult.value();

        if (!name.equals(getName(msg.senderAddress())))
            return error(Result.ErrorCode.FORBIDDEN);

        Message token = new Message();
        token.setSubject("DELETE");
        token.setId(mid);
        token.setSender(msg.getSender());
        token.setDestination(msg.getDestination());

        long offset = publisher.publish(myKafkaTopic, gson.toJson(token));
        if (offset < 0) return error(INTERNAL_ERROR);

        SyncPoint.getSyncPoint().waitForResult(offset);
        VersionHeaderHandler.version.set(offset);
        return ok();
    }

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        Log.info(() -> "remotePostMessage (rep): msg=%s\n".formatted(msg));

        // Publish to local Kafka so ALL replicas in this domain receive the remote message.
        // Without this, only the replica that received the REST call would store it.
        long offset = publisher.publish(myKafkaTopic, gson.toJson(msg));
        if (offset < 0) return error(INTERNAL_ERROR);

        SyncPoint.getSyncPoint().waitForResult(offset);
        return ok();
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        Log.info(() -> "remoteDeleteMessage (rep): mid=%s\n".formatted(mid));

        // Build a DELETE token and publish to Kafka so all replicas apply the delete.
        Message token = new Message();
        token.setSubject("DELETE");
        token.setId(mid);
        token.setSender(THIS_DOMAIN); // not a local address, so remote propagation is skipped

        long offset = publisher.publish(myKafkaTopic, gson.toJson(token));
        if (offset < 0) return error(INTERNAL_ERROR);

        SyncPoint.getSyncPoint().waitForResult(offset);
        return ok();
    }

    private void processKafkaEvent(ConsumerRecord<String, String> record) {
        long offset = record.offset();
        Message msg = gson.fromJson(record.value(), Message.class);

        // --- Token DELETE ---
        if ("DELETE".equals(msg.getSubject())) {
            deleteFromLocalInbox(msg.getId());

            // Só a réplica cujo sender é local propaga para domínios remotos
            if (super.isLocalAddress(msg.senderAddress())) {
                msg.getDestination().stream()
                        .map(r -> r.split("@")[1])
                        .filter(d -> !d.equals(IP.domain()))
                        .collect(Collectors.toSet())
                        .forEach(domain -> jobs.submit(domain, () ->
                                retryAcrossReplicas(domain, r -> r.remoteDeleteMessage(msg.getId()))));
            }
            VersionHeaderHandler.version.set(offset);
            SyncPoint.getSyncPoint().setResult(offset, msg.getId());
            return;
        }

        if ("REMOVE".equals(msg.getSubject())) {
            DB.transaction(h -> h.deleteOne(new InboxEntry(msg.getId(), msg.getSender())).mapToVoid());
            gcDeletedMessageCache.put(msg.getId(), msg.getId());
            VersionHeaderHandler.version.set(offset);
            SyncPoint.getSyncPoint().setResult(offset, msg.getId());
            return;
        }

        String senderDomain = super.getDomain(msg.senderAddress());

        if (!super.isLocalDomain(senderDomain)) {
            // Deduplicate: if we've already processed this remote message ID, skip it.
            // Using a set is correct because remote messages can arrive out of order
            // (multiple replicas of the sending domain may all forward the same message).
            if (seenRemoteMessageIds.putIfAbsent(msg.getId(), Boolean.TRUE) != null) {
                SyncPoint.getSyncPoint().setResult(offset, msg.getId());
                return;
            }
        }

        if (super.isLocalDomain(senderDomain)) {
            msg.setId("%s+%04d".formatted(THIS_DOMAIN, offset));
        }
        messagesCache.put(msg.getId(), msg);

        // Entrega local
        var localAddrs = getLocalRecipientAddresses(msg);
        if (!localAddrs.isEmpty())
            postToLocalInboxes(localAddrs, msg);

        // Propagação remota — só feita pela réplica cujo sender é local
        var remoteAddrs = getRemoteRecipientAddresses(msg);
        if (!remoteAddrs.isEmpty() && super.isLocalAddress(msg.senderAddress())) {
            remoteAddrs.stream()
                    .collect(Collectors.groupingBy(super::getDomain, Collectors.toSet()))
                    .forEach((domain, addrs) ->
                            jobs.submit(domain, () -> {
                                var res = retryAcrossReplicas(domain, r -> r.remotePostMessage(msg));
                                if (res.error() == ErrorCode.TIMEOUT)
                                    addrs.forEach(addr ->
                                            postToLocalInboxes(
                                                    Set.of(msg.senderAddress()),
                                                    msg.cloneWithTimeout(addr)));
                            })
                    );
        }

        VersionHeaderHandler.version.set(offset);
        SyncPoint.getSyncPoint().setResult(offset, msg.getId());
    }


    /**
     * Retries a remote operation across all known replicas of a domain.
     * When a replica is down, tries the next one instead of retrying the same dead replica.
     * This ensures fault tolerance during replica failures.
     */
    private <T> Result<T> retryAcrossReplicas(String domain, java.util.function.Function<sd2526.trab.impl.api.java.AdminMessages, Result<T>> operation) {
        var sn = "%s@%s".formatted(sd2526.trab.api.java.Messages.SERVICE_NAME, domain);
        long deadline = System.currentTimeMillis() + 90000;

        while (System.currentTimeMillis() < deadline) {
            var uris = sd2526.trab.impl.discovery.Discovery.getInstance().knownUrisOf(sn, 0);
            java.util.Collections.shuffle(java.util.Arrays.asList(uris));

            for (var uri : uris) {
                try {
                    java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
                    java.util.concurrent.Future<Result<T>> future = executor.submit(() -> {
                        var client = Clients.AdminMessagesClient.get(uri);
                        return operation.apply(client);
                    });

                    Result<T> res = future.get(2, java.util.concurrent.TimeUnit.SECONDS);
                    executor.shutdownNow();

                    if (res.isOK() || (res.error() != Result.ErrorCode.TIMEOUT && res.error() != Result.ErrorCode.INTERNAL_ERROR)) {
                        return res;
                    }
                } catch (Exception e) {
                    // This replica is dead or slow. Continue to next URI immediately.
                }
            }
            sd2526.trab.impl.utils.Sleep.ms(200);
        }
        return Result.error(Result.ErrorCode.TIMEOUT);
    }

    private static volatile JavaMessagesRep instance;

    public static synchronized JavaMessagesRep getInstance() {
        if (instance == null)
            instance = new JavaMessagesRep();
        return instance;
    }
}