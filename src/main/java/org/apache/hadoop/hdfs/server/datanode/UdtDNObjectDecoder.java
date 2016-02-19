package org.apache.hadoop.hdfs.server.datanode;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.server.datanode.udt.codec.ProtobufDecoder;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.apache.hadoop.hdfs.server.datanode.UdtDNObjectDecoder.State;

/**
 * 用于异步非阻塞的datanode交互协议实现
 * @author taojiaen
 *
 */
public abstract class UdtDNObjectDecoder extends ReplayingDecoder<State>{
	public static final Log LOG = DataNode.LOG;

	protected final DataNode datanode;
	/**
	 * 处理操作数的数量
	 */
	private int 	opsProcessed = 0;
	private final ProtobufDecoder protobufDecoder = new ProtobufDecoder();

	public UdtDNObjectDecoder(DataNode datanode) {
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
			            throw err;
			          }
				}
				return;
			}
			break;
		case OP_WRITE_BLOCK:
			try {
			final OpWriteBlockProto proto = (OpWriteBlockProto) protobufDecoder.decode(in);
			if (proto != null) {
				opWriteBlock(proto, ctx,in);
			}
			} catch(Throwable t) {

				throw t;
			}finally {
				reset();
			}
			break;
		default: break;
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
		checkpoint(State.INITIAL_TYPE);
	}


	private void sloveProccessingError(Op op, Throwable t, ChannelHandlerContext ctx) {
		String s = datanode.getDisplayName() + ":DataXceiver error processing " + ((op == null) ? "unknown" : op.name())
				+ " operation " + " src: " + ctx.channel().remoteAddress().toString() + " dst: " + localAddress;
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
	 * 处理读数据块
	 * @param ctx
	 * @param in
	 * @param out
	 */
	protected abstract void opWriteBlock(final OpWriteBlockProto proto, final ChannelHandlerContext ctx, final ByteBuf in);



	/**
	 * 记录错误
	 * @param ctx
	 */
	private void incrDatanodeNetworkErrors(final ChannelHandlerContext ctx) {
		datanode.incrDatanodeNetworkErrors(ctx.channel().remoteAddress().toString());
	}

	/**
	 * The internal state of {@link UdtDNObjectDecoder}.
	 * <em>Internal use only</em>.
	 */
	protected static enum State {
		INITIAL_TYPE,
		OP_WRITE_BLOCK,
		OP_WRITE_BLOCK_INIT,
		OP_READ_BLOCK,
	    CHUNKED_WRITE_BLOCK,
	    BAD_MESSAGE,
	    UPGRADED
	}
}