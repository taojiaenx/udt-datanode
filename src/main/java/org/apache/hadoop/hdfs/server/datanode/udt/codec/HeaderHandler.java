package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.datatransfer.Op;

public class HeaderHandler extends SimpleChannelInboundHandler<ByteBuf> implements UDTDataNodeChannelHandler {

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
		if (ctx.attr(CHANNEL_MODE).get() == UDTChannelMode.READ) {
			ctx.fireChannelRead(msg);
		} else {
			processHeadBuf(ctx, msg);
		}
	}

	private void processHeadBuf(ChannelHandlerContext ctx, ByteBuf msg) {
		switch(createOpAndSkipLengthField(msg)) {
	    case READ_BLOCK:
	      opReadBlock(msg);
	      break;
	    case WRITE_BLOCK:
	      opWriteBlock(in);
	      break;
	    case REPLACE_BLOCK:
	      opReplaceBlock(in);
	      break;
	    case COPY_BLOCK:
	      opCopyBlock(in);
	      break;
	    case BLOCK_CHECKSUM:
	      opBlockChecksum(in);
	      break;
	    case TRANSFER_BLOCK:
	      opTransferBlock(in);
	      break;
	    case REQUEST_SHORT_CIRCUIT_FDS:
	      opRequestShortCircuitFds(in);
	      break;
	    case RELEASE_SHORT_CIRCUIT_FDS:
	      opReleaseShortCircuitFds(in);
	      break;
	    case REQUEST_SHORT_CIRCUIT_SHM:
	      opRequestShortCircuitShm(in);
	      break;
	    default:
	      throw new IOException("Unknown op " + op + " in data stream");
	    }
	}

	/**
	 * 读任务
	 */
	private void opReadBlock(ByteBuf msg) {

	}

	private static Op createOpAndSkipLengthField(ByteBuf msg) {
		final Op op = Op.valueOf(msg.readByte());
		// 不再读取头部的数据
		msg.skipBytes(LENGTH_FIELD_LENGETH);
		return op;
	}

}
