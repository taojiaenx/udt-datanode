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
 *
 * @author taojiaen
 *
 */
public abstract class FileUploadDecoder extends SimpleChannelInboundHandler<PacketReceiver> {
	private  packetBufferReader dataReader;
	private  packetBufferReader checksumReader;

	public FileUploadDecoder() {

	}
	public FileUploadDecoder(final FileChannel fileChannel, FileChannel checksumChannel) {
		this.dataReader = new packetBufferReader(fileChannel);
		this.checksumReader = new packetBufferReader(checksumChannel);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, PacketReceiver msg) throws Exception {
		parsePacketRecived(ctx, msg, dataReader.fileByteQueue, checksumReader.fileByteQueue);
		doflush(ctx);
	}

	protected abstract void parsePacketRecived(ChannelHandlerContext ctx, PacketReceiver msg, Queue<ByteBuf> data,
			Queue<ByteBuf> checksum);

	protected abstract void dataSuccess(long len);

	protected abstract void checksumfileSuccess(long len);


	/**
	 * 手动调用写入结束处理
	 */
	protected void finishRead() {
		closeReader();
	};

	/**
	 * 同步文件,强制写入到磁盘
	 *
	 * @throws IOException
	 */
	protected void syncFile() throws IOException {
		dataReader.fileChannel.force(true);
		checksumReader.fileChannel.force(true);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		closeReader();
		ctx.fireChannelInactive();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		closeReader();
		if (ctx.channel().isActive()) {
			ctx.fireExceptionCaught(cause);
		}
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		closeReader();
	}

	private void closeReader() {
		dataReader.closeAll();
		checksumReader.closeAll();
	}

	protected void setDataReader(FileChannel channel) {
		this.dataReader = new packetBufferReader(channel);
	}
	protected void setChecksumReader(FileChannel channel) {
		this.checksumReader = new packetBufferReader(channel);
	}
	/**
	 * 把数据写入到文件中
	 *
	 * @param ctx
	 * @throws IOException
	 */
	private void doflush(final ChannelHandlerContext ctx) throws IOException {
		long fileSize = dataReader.flushFileQueue(ctx);
		dataSuccess(fileSize);
		long checksumsize = checksumReader.flushFileQueue(ctx);
		checksumfileSuccess(checksumsize);
		if (dataReader.canSolveData() || checksumReader.canSolveData()) {
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

/**
 * 把packet传入文件中
 *
 * @author taojiaen
 *
 */
class packetBufferReader {
	final FileChannel fileChannel;
	final Queue<ByteBuf> fileByteQueue = new ArrayDeque<ByteBuf>();
	private ByteBuf currentByteBuf = null;
	private long currentWritelen = 0;

	public packetBufferReader(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	void recieveBuf(ByteBuf buf) {
		fileByteQueue.add(buf);
	}

	/**
	 * 把数据写入到文件中
	 *
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

			// 如果buffer已经写完了
			if (!currentByteBuf.isReadable()) {
				ReferenceCountUtil.release(currentByteBuf);
				currentByteBuf = null;
			}
			if (currentReadlen <= 0) {
				break;
			} else {
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
	 *
	 * @param queue
	 */
	private static void clearBufQueue(Queue<ByteBuf> queue) {
		ByteBuf buf = null;
		while (queue.size() > 0) {
			buf = queue.poll();
			if (buf != null) {
				ReferenceCountUtil.release(buf);
			} else {
				break;
			}
		}
	}

}
