package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import java.nio.ByteOrder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * 用户操作解析
 * @author taojiaen
 *
 */
public class UDTMessageFormater extends LengthFieldBasedFrameDecoder implements UDTDataNodeChannelHandler{

	public UDTMessageFormater() {
		super(Short.MAX_VALUE, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGETH);
		// TODO Auto-generated constructor stub
	}

	 @Override
	    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		 if (ctx.attr(CHANNEL_MODE).get() == UDTChannelMode.READ) {
			 ctx.fireChannelRead(msg);
		 } else {
			 super.channelRead(ctx, msg);
		 }
	    }
	 /**
	  * 特制的长度算法
	  */
	 @Override
	 protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
	        buf = buf.order(order);
	    }
}
