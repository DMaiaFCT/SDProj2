package sd2526.trab.impl.grpc.servers;

import static sd2526.trab.impl.grpc.common.DataModelAdaptor.GrpcAdminMessage_to_Message;

import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.grpc.generated_java.AdminMessagesProtoBuf.GrpcAdminMessage;
import sd2526.trab.impl.grpc.generated_java.AdminMessagesProtoBuf.RemoteDeleteMessageArgs;
import sd2526.trab.impl.grpc.generated_java.GrpcAdminMessagesGrpc;
import sd2526.trab.impl.java.servers.JavaMessages;

public class GrpcAdminMessagesController extends GrpcController implements GrpcAdminMessagesGrpc.AsyncService {

    AdminMessages impl = JavaMessages.getInstance();

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcAdminMessagesGrpc.bindService(this);
    }

    private void requireServerSecret() {
        String expectedSecret = System.getProperty("service.secret");
        String providedSecret = AbstractGrpcServer.SECRET_CONTEXT_KEY.get();

        if (expectedSecret != null && !expectedSecret.equals(providedSecret)) {
            throw Status.PERMISSION_DENIED.withDescription("Blocked unauthorized access attempt.").asRuntimeException();
        }
    }

    @Override
    public void remotePostMessage(GrpcAdminMessage request, StreamObserver<Empty> responseObserver) {
        try {
            requireServerSecret();
            super.toGrpcResult(responseObserver,
                    ((AdminMessages)impl).remotePostMessage( GrpcAdminMessage_to_Message(request)),
                    (__) -> Empty.newBuilder().build());
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void remoteDeleteMessage(RemoteDeleteMessageArgs request, StreamObserver<Empty> responseObserver) {
        try {
            requireServerSecret();
            super.toGrpcResult(responseObserver,
                    ((AdminMessages)impl).remoteDeleteMessage(  request.getMid() ),
                    (__) -> Empty.newBuilder().build());
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}