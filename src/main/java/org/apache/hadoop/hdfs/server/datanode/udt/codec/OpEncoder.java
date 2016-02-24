package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;

/**
 * 发送用户的operation
 * @author taojiaen
 *
 */
public class OpEncoder extends MessageToByteEncoder<Op>{

	@Override
	protected void encode(ChannelHandlerContext ctx, Op operation, ByteBuf out) throws Exception {
		out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
		out.writeByte(operation.code);
	}

}
