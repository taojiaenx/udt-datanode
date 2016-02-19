package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.UdtDNObjectDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * datanode 请求解析器
 * @author taojiaen
 *
 */
public class UdtDNRequestDecoder extends UdtDNObjectDecoder{

	public UdtDNRequestDecoder(DataNode datanode) {
		super(datanode);
	}

	@Override
	protected void opWriteBlock(final OpWriteBlockProto proto, final ChannelHandlerContext ctx,
			final ByteBuf in) {
		
	}

}
