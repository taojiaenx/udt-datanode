package org.apache.hadoop.hdfs.server.datanode;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.udt.nio.NioUdtByteConnectorChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.PeerServer;

/**
 * 以udt模式启动的数据节点服务器
 * @author taojiaen
 *
 */
class UDTDataXceiverServer extends DataXceiverServer{
	public static final Log log = LogFactory.getLog(UDTDataXceiverServer.class);
    final ThreadFactory acceptFactory = new UtilThreadFactory("accept");
    final ThreadFactory connectFactory = new UtilThreadFactory("connect");
	ServerBootstrap serverBootstarp = null;
	EventLoopGroup bossGroup = null;
	EventLoopGroup workerGroup = null;

	UDTDataXceiverServer(PeerServer peerServer, Configuration conf,
			DataNode datanode) {
		super(peerServer, conf, datanode);
		log.info("udt服务器构造成功");
	}
	@Override
	  public void run() {
		log.info("开始工作");
    	this.bossGroup = new NioEventLoopGroup(1,acceptFactory, NioUdtProvider.BYTE_PROVIDER);
    	this.workerGroup = new NioEventLoopGroup(3, connectFactory, NioUdtProvider.BYTE_PROVIDER);
    	serverBootstarp = new ServerBootstrap()
		.group(bossGroup, workerGroup).channelFactory(NioUdtProvider.BYTE_ACCEPTOR).childHandler(new ChannelInitializer<NioUdtByteConnectorChannel>(){

			@Override
			protected void initChannel(NioUdtByteConnectorChannel ch)
					throws Exception {
				ch.pipeline().addLast(
                        new LoggingHandler(LogLevel.INFO),
                        new ModemServerHandler());
			}
			
		});

         try {
			serverBootstarp.bind(1013).sync();
		} catch (InterruptedException e) {
			log.error("绑定出错", e);
		}
         log.info("绑定成功");
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

    private static final Log log = LogFactory.getLog(ModemServerHandler.class.getName());
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    log.info("server active");
    System.out.println("channelActive===================");
        log.info("ECHO active " + NioUdtProvider.socketUDT(ctx.channel()).toStringOptions());
    }
    
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
    log.info("channelRead===================");
        ByteBuf buf = (ByteBuf) msg;
        try {
        StringBuilder sb=new StringBuilder();
            for (int i = 0; i < buf.capacity(); i ++) {
                byte b = buf.getByte(i);
                sb.append((char) b);
            }
            log.info(sb.toString());
            
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
        log.warn("close the connection when an exception is raised", cause);
        ctx.close();
    }

}