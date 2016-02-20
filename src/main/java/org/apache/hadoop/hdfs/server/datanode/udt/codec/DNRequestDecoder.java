package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DNObjectDecoder;
import org.apache.hadoop.security.token.Token;
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

	public DNRequestDecoder(DataNode datanode) {
		super(datanode);
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
	}



}
