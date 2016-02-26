package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;

/**
 * 用于处理文件上传的handler
 * @author taojiaen
 *
 */
public abstract class BlockWriterDecoder extends SimpleChannelInboundHandler<PacketReceiver>{
	private final FileWriter dataWriter;
	private final FileWriter checksumWriter;

	public BlockWriterDecoder(final FileChannel fileChannel, FileChannel checksumChannel) {
		this.dataWriter = new FileWriter(fileChannel);
		this.checksumWriter = new FileWriter(checksumChannel);
	}



	@Override
	protected void channelRead0(ChannelHandlerContext ctx, PacketReceiver msg) throws Exception {
		dataWriter.recieveBuf(getfileBuf(msg));
		checksumWriter.recieveBuf(getchecksumBuf(msg));
		doflush(ctx);
	}

	protected abstract ByteBuf getfileBuf(PacketReceiver msg);
	protected abstract ByteBuf getchecksumBuf(PacketReceiver msg);
	protected abstract void dataSuccess(long len);
	protected abstract void checksumfileSuccess(long len);
	protected abstract boolean isEndofWrite();
	@Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		closeWriter();
        ctx.fireChannelInactive();
    }

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		closeWriter();
		if (ctx.channel().isActive()) {
			ctx.fireExceptionCaught(cause);
		}
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		closeWriter();
	}


	private void closeWriter() {
		dataWriter.closeAll();
		checksumWriter.closeAll();
	}



	private void doflush(final ChannelHandlerContext ctx) throws IOException {
		long fileSize = dataWriter.flushFileQueue(ctx);
		dataSuccess(fileSize);
		long checksumsize = checksumWriter.flushFileQueue(ctx);
		checksumfileSuccess(checksumsize);
		if (isEndofWrite()) {
			closeWriter();
		} else if (dataWriter.canSolveData() || checksumWriter.canSolveData()) {
			ctx.executor().execute(new Runnable() {

				@Override
				public void run() {

					try {
						doflush(ctx);
					} catch (IOException e) {
						try {
							exceptionCaught(ctx, e);
						} catch (Exception e1) {
						}
					}
				}

			});
		}
	}

}

class FileWriter {
	private final FileChannel fileChannel;
	private final Queue<ByteBuf> fileByteQueue = new ArrayDeque<ByteBuf>();
	private ByteBuf currentByteBuf = null;
	private long currentWritelen = 0;

	public FileWriter(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	void recieveBuf(ByteBuf buf) {
		fileByteQueue.add(buf);
	}
	/**
	 * 把数据写入到文件中
	 * @param ctx
	 * @return
	 * @throws IOException
	 */
	long flushFileQueue(ChannelHandlerContext ctx) throws IOException {
		long readlen = 0;
		long currentReadlen = 0;
		currentWritelen = 0;
		if (!fileChannel.isOpen())
			return 0;
		currentByteBuf.readBytes(fileChannel, currentByteBuf.readableBytes());
		while (true) {
			// 从nettyBuffer中读入数据
			if (currentByteBuf == null) {
				currentByteBuf = fileByteQueue.poll();
			}

			if (currentByteBuf == null) {
				break;
			}

			// 把数据写入文件
			currentReadlen = 0;
			if (currentByteBuf.isReadable()) {
				currentReadlen = currentByteBuf.readBytes(fileChannel, currentByteBuf.readableBytes());
			}

			//如果buffer已经写完了
			if (!currentByteBuf.isReadable()) {
				ReferenceCountUtil.release(currentByteBuf);
				currentByteBuf = null;
			}
			if (currentReadlen <= 0) {
				break;
			} else {
				/**
				 * 强制刷入磁盘
				 */
				fileChannel.force(true);
				readlen += currentReadlen;
			}
		}
		currentWritelen = readlen;
		return readlen;
	}

	/**
	 * 是否仍旧可以处理数据
	 */
	boolean canSolveData() {
		return fileChannel.isOpen() && currentWritelen > 0
				&& ((currentByteBuf != null && currentByteBuf.isReadable()) || fileByteQueue.size() > 0);
	}


	void closeAll() {
		try {
			if (fileChannel != null && fileChannel.isOpen()) {
				fileChannel.close();
			}
		} catch (IOException e) {
		}
		clearBufQueue(fileByteQueue);
		if (currentByteBuf != null) {
			ReferenceCountUtil.release(currentByteBuf);
		}
	}


	/***
	 * 清理buffer队列
	 * @param queue
	 */
	private static void  clearBufQueue(Queue<ByteBuf> queue) {
		ByteBuf buf = null;
		while(queue.size() > 0) {
			buf = queue.poll();
			if (buf != null) {
				ReferenceCountUtil.release(buf);
			} else {
				break;
			}
		}
	}

}
