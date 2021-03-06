package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.continueTraceSpan;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.fromProto;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.udt.codec.ProtobufDecoder;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.TraceScope;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.apache.hadoop.hdfs.server.datanode.DNObjectDecoder.State;

/**
 * 用于异步非阻塞的datanode交互协议实现
 * @author taojiaen
 *
 */
public abstract class DNObjectDecoder extends ReplayingDecoder<State>{
	protected static final Log LOG = DataNode.LOG;
	protected static final String DATA_PACKET_SOLVER = "DATA_PACKET_SOLVER";

	protected final DataNode datanode;
	/**
	 * 处理操作数的数量
	 */
	private int 	opsProcessed = 0;
	private final ProtobufDecoder protobufDecoder = new ProtobufDecoder();
	/**
	 * 用于测试用的TraceScope
	 */
	private TraceScope traceScope = null;

	public DNObjectDecoder(DataNode datanode) {
		super(State.INITIAL_TYPE);
		this.datanode = datanode;
	}

	@Override
	protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
		switch(state()) {
		case INITIAL_TYPE:
			if (in.readableBytes() > 3) {
				try {
					checkpoint(readOp(in));
				} catch (IOException err) {
					checkpoint(State.INITIAL_TYPE);
					LOG.error(err);
					 // Since we optimistically expect the next op, it's quite normal to get EOF here.
			          if (opsProcessed > 0 &&
			              (err instanceof EOFException || err instanceof ClosedChannelException)) {
			            if (LOG.isDebugEnabled()) {
			              LOG.debug("Cached " + this + " closing after " + opsProcessed + " ops");
			            }
			          } else {
			            incrDatanodeNetworkErrors(ctx);
			            ctx.close();
			          }
				}
				return;
			}
			break;
		case OP_WRITE_BLOCK:
			ctx.pipeline().remove(DATA_PACKET_SOLVER);

			traceScope = null;
			try {
				final OpWriteBlockProto proto = (OpWriteBlockProto) protobufDecoder.decode(in);
				if (proto != null) {
					final DatanodeInfo[] targets = PBHelper.convert(proto.getTargetsList());
					traceScope = continueTraceSpan(proto.getHeader(), proto.getClass().getSimpleName());
					writeBlock(PBHelper.convert(proto.getHeader().getBaseHeader().getBlock()),
							PBHelper.convertStorageType(proto.getStorageType()),
							PBHelper.convert(proto.getHeader().getBaseHeader().getToken()),
							proto.getHeader().getClientName(), targets,
							PBHelper.convertStorageTypes(proto.getTargetStorageTypesList(), targets.length),
							PBHelper.convert(proto.getSource()), fromProto(proto.getStage()), proto.getPipelineSize(),
							proto.getMinBytesRcvd(), proto.getMaxBytesRcvd(), proto.getLatestGenerationStamp(),
							fromProto(proto.getRequestedChecksum()),
							(proto.hasCachingStrategy() ? getCachingStrategy(proto.getCachingStrategy())
									: CachingStrategy.newDefaultStrategy()),
							(proto.hasAllowLazyPersist() ? proto.getAllowLazyPersist() : false),
							(proto.hasPinning() ? proto.getPinning() : false),
							(PBHelper.convertBooleanList(proto.getTargetPinningsList())),ctx);
				    beginWrite();
				}
			} catch (Throwable t) {
				sloveProccessingError(Op.WRITE_BLOCK, t, ctx);
				ctx.close();
			}
			break;
		case CHUNKED_WRITE_BLOCK:
			if (isWriteOperationInitfinished(ctx)) {
				out.add(in);
			} else {
				return;
			}
			break;
		default:
			break;
		}
	}

	protected State readOp(ByteBuf in) throws IOException {
		final short version = in.readShort();
		if (version != DataTransferProtocol.DATA_TRANSFER_VERSION) {
			throw new IOException("Version Mismatch (Expected: " + DataTransferProtocol.DATA_TRANSFER_VERSION
					+ ", Received: " + version + " )");
		}
		final Op op = Op.valueOf(in.readByte());
		switch (op) {
		case READ_BLOCK:
			return State.OP_READ_BLOCK;

		case WRITE_BLOCK:
			protobufDecoder.setPrototype(OpWriteBlockProto.getDefaultInstance());
			return State.OP_WRITE_BLOCK;
		default:
			throw new IOException("Unknown op " + op + " in data stream");
		}
	}


	protected void  reset() {
		if (traceScope != null) {
			traceScope.close();
		}
		checkpoint(State.INITIAL_TYPE);
	}


	private void beginWrite() {
		checkpoint(State.CHUNKED_WRITE_BLOCK);
	}

	private void sloveProccessingError(Op op, Throwable t, ChannelHandlerContext ctx) {
		String s = datanode.getDisplayName() + ":DataXceiver error processing " + ((op == null) ? "unknown" : op.name())
				+ " operation " + " src: " + ctx.channel().remoteAddress().toString();
		if (op == Op.WRITE_BLOCK && t instanceof ReplicaAlreadyExistsException) {
			// For WRITE_BLOCK, it is okay if the replica already exists since
			// client and replication may write the same block to the same
			// datanode
			// at the same time.
			if (LOG.isTraceEnabled()) {
				LOG.trace(s, t);
			} else {
				LOG.info(s + "; " + t);
			}
		} else if (op == Op.READ_BLOCK && t instanceof SocketTimeoutException) {
			String s1 = "Likely the client has stopped reading, disconnecting it";
			s1 += " (" + s + ")";
			if (LOG.isTraceEnabled()) {
				LOG.trace(s1, t);
			} else {
				LOG.info(s1 + "; " + t);
			}
		} else {
			LOG.error(s, t);
		}
	}


/**
 * 协数据块操作
 * @param block
 * @param storageType
 * @param blockToken
 * @param clientname
 * @param targets
 * @param targetStorageTypes
 * @param srcDataNode
 * @param stage
 * @param pipelineSize
 * @param minBytesRcvd
 * @param maxBytesRcvd
 * @param latestGenerationStamp
 * @param requestedChecksum
 * @param cachingStrategy
 * @param allowLazyPersist
 * @param pinning
 * @param targetPinnings
 * @param ctx
 * @param in
 * @param out
 * @throws IOException
 */
	protected abstract void writeBlock(final ExtendedBlock block,
		      final StorageType storageType,
		      final Token<BlockTokenIdentifier> blockToken,
		      final String clientname,
		      final DatanodeInfo[] targets,
		      final StorageType[] targetStorageTypes,
		      final DatanodeInfo srcDataNode,
		      final BlockConstructionStage stage,
		      final int pipelineSize,
		      final long minBytesRcvd,
		      final long maxBytesRcvd,
		      final long latestGenerationStamp,
		      DataChecksum requestedChecksum,
		      CachingStrategy cachingStrategy,
		      final boolean allowLazyPersist,
		      final boolean pinning,
		      final boolean[] targetPinnings,
		      final ChannelHandlerContext ctx) throws IOException;

	protected abstract boolean isWriteOperationInitfinished(final ChannelHandlerContext ctx);

	static private CachingStrategy getCachingStrategy(CachingStrategyProto strategy) {
	    Boolean dropBehind = strategy.hasDropBehind() ?
	        strategy.getDropBehind() : null;
	    Long readahead = strategy.hasReadahead() ?
	        strategy.getReadahead() : null;
	    return new CachingStrategy(dropBehind, readahead);
	  }



	/**
	 * 记录错误
	 * @param ctx
	 */
	protected void incrDatanodeNetworkErrors(final ChannelHandlerContext ctx) {
		incrDatanodeNetworkErrors(ctx.channel());
	}
	/**
	 * 记录错误
	 * @param ctx
	 */
	protected void incrDatanodeNetworkErrors(final Channel channel) {
		datanode.incrDatanodeNetworkErrors(channel.remoteAddress().toString());
	}

	@Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
		reset();
        super.exceptionCaught(ctx, cause);
    }
	@Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		reset();
        super.channelInactive(ctx);
    }

	/**
	 * The internal state of {@link DNObjectDecoder}.
	 * <em>Internal use only</em>.
	 */
	protected static enum State {
		INITIAL_TYPE,
		OP_WRITE_BLOCK,
		OP_READ_BLOCK,
	    CHUNKED_WRITE_BLOCK,
	    BAD_MESSAGE,
	    UPGRADED
	}
}