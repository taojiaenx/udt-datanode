package org.apache.hadoop.hdfs.server.datanode;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.PeerServer;

/**
 * 以udt模式启动的数据节点服务器
 * @author taojiaen
 *
 */
class UDTDataXceiverServer extends DataXceiverServer{
    final ThreadFactory acceptFactory = new UtilThreadFactory("accept");
    final ThreadFactory connectFactory = new UtilThreadFactory("connect");
	ServerBootstrap serverBootstarp = null;
	EventLoopGroup bossGroup = null;
	EventLoopGroup workerGroup = null;

	UDTDataXceiverServer(PeerServer peerServer, Configuration conf,
			DataNode datanode) {
		super(peerServer, conf, datanode);
	}
	@Override
	  public void run() {
		
	}


}
class UtilThreadFactory implements ThreadFactory {

    private static final AtomicInteger counter = new AtomicInteger();

    private final String name;

    public UtilThreadFactory(final String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        return new Thread(runnable, name + '-' + counter.getAndIncrement());
    }
}
class ModemServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = Logger.getLogger(ModemServerHandler.class.getName());
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    log.info("server active");
    System.out.println("channelActive===================");
        log.info("ECHO active " + NioUdtProvider.socketUDT(ctx.channel()).toStringOptions());
    }
    
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
    System.out.println("channelRead===================");
        ByteBuf buf = (ByteBuf) msg;
        try {
        StringBuilder sb=new StringBuilder();
            for (int i = 0; i < buf.capacity(); i ++) {
                byte b = buf.getByte(i);
                sb.append((char) b);
            }
            System.out.println(sb.toString());
            
        } catch(Exception e){
        e.printStackTrace();
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
        //ctx.write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx,
            final Throwable cause) {
        log.log(Level.WARNING, "close the connection when an exception is raised", cause);
        ctx.close();
    }

}