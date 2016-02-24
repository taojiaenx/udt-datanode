package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.udt.codec.BlockReciverDecoder;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.DataChecksum;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * datanode 请求解析器
 * @author taojiaen
 *
 */
public class DNRequestDecoder extends DNObjectDecoder{
	private static final String DATA_PACKET_SOLVER = "DATA_PACKET_SOLVER";
	private String previousOpClientName;
	final long estimateBlockSize;
	private Bootstrap mirrorBoot;
	private final boolean connectToDnViaHostname;
	private DataOutputStream mirrorOut = null; // stream to next target
	private DataInputStream mirrorIn = null; // reply from next target
	private Socket mirrorSock = null; // socket to next target
	private String mirrorNode = null; // the name:port of next target
	private String firstBadLink = ""; // first datanode that failed in
								// connection setup
	Status mirrorInStatus = SUCCESS;

	public DNRequestDecoder(DataNode datanode, Configuration conf,
			Bootstrap mirrorBoot) {
		super(datanode);
		this.estimateBlockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
		        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
		this.mirrorBoot = mirrorBoot;
		this.connectToDnViaHostname = datanode.getDnConf().connectToDnViaHostname;
	}

	/**
	 * 写方法的处理
	 */
	@Override
	protected void writeBlock(final ExtendedBlock block,
		      final StorageType storageType,
		      final Token<BlockTokenIdentifier> blockToken,
		      final String clientname,
		      final DatanodeInfo[] targets,
		      final StorageType[] targetStorageTypes,
		      final DatanodeInfo srcDataNode,
		      final BlockConstructionStage stage,
		      final int pipelineSize,
		      final long minBytesRcvd,
		      final long maxBytesRcvd,
		      final long latestGenerationStamp,
		      final DataChecksum requestedChecksum,
		      final CachingStrategy cachingStrategy,
		      final boolean allowLazyPersist,
		      final boolean pinning,
		      final boolean[] targetPinnings,
 final ChannelHandlerContext ctx, ByteBuf in,
			List<Object> out) throws IOException {
		previousOpClientName = clientname;
		final boolean isDatanode = clientname.length() == 0;
		final boolean isClient = !isDatanode;
		final boolean isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
				|| stage == BlockConstructionStage.TRANSFER_FINALIZED;
		long size = 0;
		// check single target for transfer-RBW/Finalized
		if (isTransfer && targets.length > 0) {
			throw new IOException(stage + " does not support multiple targets " + Arrays.asList(targets));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("opWriteBlock: stage=" + stage + ", clientname=" + clientname + "\n  block  =" + block
					+ ", newGs=" + latestGenerationStamp + ", bytesRcvd=[" + minBytesRcvd + ", " + maxBytesRcvd + "]"
					+ "\n  targets=" + Arrays.asList(targets) + "; pipelineSize=" + pipelineSize + ", srcDataNode="
					+ srcDataNode + ", pinning=" + pinning);
			LOG.debug("isDatanode=" + isDatanode + ", isClient=" + isClient + ", isTransfer=" + isTransfer);
			/*
			 * LOG.debug("writeBlock receive buf size " +
			 * peer.getReceiveBufferSize() + " tcp no delay " +
			 * peer.getTcpNoDelay());
			 */
		}

		final ExtendedBlock originalBlock = new ExtendedBlock(block);
		if (block.getNumBytes() == 0) {
			// 如果这个块是空的，就写入默认的块大小
			block.setNumBytes(estimateBlockSize);
		}
		LOG.info("Receiving " + block + " src: " + ctx.channel().remoteAddress() + " dest: "
				+ ctx.channel().localAddress());
		checkAccess(ctx, isClient, block, blockToken, Op.WRITE_BLOCK, BlockTokenSecretManager.AccessMode.WRITE);

		final Runnable runnalbe = new Runnable() {

			@Override
			public void run() {
				try {



					final String storageUuid;
					if (isDatanode || stage != BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
						// open a block receiver
						// BlockReceiver是用来接收具体数据的类，构造函数的具体工作是建立到块文件和meta文件的流
						final BlockReciverDecoder blockReceiver = new BlockReciverDecoder(block, storageType,
								ctx.channel(),

								// 连接的客户端的地址
								ctx.channel().remoteAddress().toString(),
								// 本地地址
								ctx.channel().localAddress().toString(), stage, latestGenerationStamp, minBytesRcvd,
								maxBytesRcvd, clientname, srcDataNode, datanode, requestedChecksum, cachingStrategy,
								allowLazyPersist, pinning);

						storageUuid = blockReceiver.getStorageUuid();
						if (ctx.pipeline().get(DATA_PACKET_SOLVER) != null) {
							ctx.pipeline().replace(DATA_PACKET_SOLVER, DATA_PACKET_SOLVER, blockReceiver);
						} else {
							ctx.pipeline().addLast(DATA_PACKET_SOLVER, blockReceiver);
						}
					} else {
						storageUuid = datanode.data.recoverClose(block, latestGenerationStamp, minBytesRcvd);
					}

					if (targets.length > 0) {
						InetSocketAddress mirrorTarget = null;
						mirrorNode = targets[0].getXferAddr(connectToDnViaHostname);
						if (LOG.isDebugEnabled()) {
							LOG.debug("Connecting to datanode " + mirrorNode);
						}

						mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
						mirrorBoot.connect(mirrorTarget).addListener(new MirrorConnectListener());
					}
				} catch (IOException e) {
				}
			}
		};
		ctx.executor().execute(runnalbe);
	}


	/**
	 * 判断客户端传来的动作是否符合权限
	 * @param ctx
	 * @param reply
	 * @param blk
	 * @param t
	 * @param op
	 * @param mode
	 * @throws IOException
	 */
	  private void checkAccess(ChannelHandlerContext ctx, final boolean reply,
	      final ExtendedBlock blk,
	      final Token<BlockTokenIdentifier> t,
	      final Op op,
	      final BlockTokenSecretManager.AccessMode mode) throws IOException {
	    if (datanode.isBlockTokenEnabled) {
	      if (LOG.isDebugEnabled()) {
	        LOG.debug("Checking block access token for block '" + blk.getBlockId()
	            + "' with mode '" + mode + "'");
	      }
	      try {
	        datanode.blockPoolTokenSecretManager.checkAccess(t, null, blk, mode);
	      } catch(InvalidToken e) {
	        try {
	          if (reply) {
	            BlockOpResponseProto.Builder resp = BlockOpResponseProto.newBuilder()
	              .setStatus(ERROR_ACCESS_TOKEN);
	            if (mode == BlockTokenSecretManager.AccessMode.WRITE) {
	              DatanodeRegistration dnR =
	                datanode.getDNRegistrationForBP(blk.getBlockPoolId());
	              // NB: Unconditionally using the xfer addr w/o hostname
	              resp.setFirstBadLink(dnR.getXferAddr());
	            }
	            //发错误消息
	            ctx.channel().writeAndFlush(resp);
	          }
	          LOG.warn("Block token verification failed: op=" + op
	              + ", remoteAddress=" + ctx.channel().remoteAddress()
	              + ", message=" + e.getLocalizedMessage());
	          throw e;
	        } finally {
	         // IOUtils.closeStream(out);
	        }
	      }
	    }
	  }

	class MirrorConnectListener implements ChannelFutureListener{

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {

			} else {
				try {
					future.get();
				} catch (Exception e) {

				}
			}
		}

	}

	  /**
	   * mirror节点ack接收
	   * @author taojiaen
	   *
	   */
	class PingMirrorDecoder  extends SimpleChannelInboundHandler<BlockOpResponseProto> {
		private final int targetlength;
		public PingMirrorDecoder(final int targetlength) {
			this.targetlength = targetlength;
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, BlockOpResponseProto connectAck)
				throws Exception {
			mirrorInStatus = connectAck.getStatus();
            firstBadLink = connectAck.getFirstBadLink();
            if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
              LOG.info("Datanode " + targetlength +
                       " got response for connect ack " +
                       " from downstream datanode with firstbadlink as " +
                       firstBadLink);
            }
		}

	};
}
