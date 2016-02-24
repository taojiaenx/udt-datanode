package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class PingmirrorDecoder extends SimpleChannelInboundHandler<BlockOpResponseProto>{

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, BlockOpResponseProto msg) throws Exception {
		// TODO Auto-generated method stub

	}

}
