package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.DataChecksum;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * datanode 请求解析器
 * @author taojiaen
 *
 */
public class DNRequestDecoder extends DNObjectDecoder{
	private String previousOpClientName;
	final long estimateBlockSize;

	public DNRequestDecoder(DataNode datanode, Configuration conf) {
		super(datanode);
		this.estimateBlockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
		        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
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
		      DataChecksum requestedChecksum,
		      CachingStrategy cachingStrategy,
		      final boolean allowLazyPersist,
		      final boolean pinning,
		      final boolean[] targetPinnings,
			ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws IOException {
	    previousOpClientName = clientname;
	    final boolean isDatanode = clientname.length() == 0;
	    final boolean isClient = !isDatanode;
	    final boolean isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
	        || stage == BlockConstructionStage.TRANSFER_FINALIZED;
	    long size = 0;
	    // check single target for transfer-RBW/Finalized
	    if (isTransfer && targets.length > 0) {
	      throw new IOException(stage + " does not support multiple targets "
	          + Arrays.asList(targets));
	    }

	    if (LOG.isDebugEnabled()) {
	      LOG.debug("opWriteBlock: stage=" + stage + ", clientname=" + clientname
	      		+ "\n  block  =" + block + ", newGs=" + latestGenerationStamp
	      		+ ", bytesRcvd=[" + minBytesRcvd + ", " + maxBytesRcvd + "]"
	          + "\n  targets=" + Arrays.asList(targets)
	          + "; pipelineSize=" + pipelineSize + ", srcDataNode=" + srcDataNode
	          + ", pinning=" + pinning);
	      LOG.debug("isDatanode=" + isDatanode
	          + ", isClient=" + isClient
	          + ", isTransfer=" + isTransfer);
	    /*  LOG.debug("writeBlock receive buf size " + peer.getReceiveBufferSize() +
	                " tcp no delay " + peer.getTcpNoDelay());*/
	    }

	    final ExtendedBlock originalBlock = new ExtendedBlock(block);
	    if (block.getNumBytes() == 0) {
	    	//如果这个块是空的，就写入默认的块大小
	      block.setNumBytes(estimateBlockSize);
	    }
	    LOG.info("Receiving " + block + " src: " + ctx.channel().remoteAddress() + " dest: "
	        + ctx.channel().localAddress());
	    checkAccess(ctx, isClient, block, blockToken,
	            Op.WRITE_BLOCK, BlockTokenSecretManager.AccessMode.WRITE);

	    DataOutputStream mirrorOut = null;  // stream to next target
	    DataInputStream mirrorIn = null;    // reply from next target
	    Socket mirrorSock = null;           // socket to next target
	    String mirrorNode = null;           // the name:port of next target
	    String firstBadLink = "";           // first datanode that failed in connection setup
	    Status mirrorInStatus = SUCCESS;
	    final String storageUuid;
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

}
