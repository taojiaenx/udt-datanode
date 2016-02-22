package org.apache.hadoop.hdfs.server.datanode.udt.codec;


import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.DataChecksum;

import io.netty.channel.Channel;

/**
 * 以异步非阻塞形式工作的BlockReceiver{@link org.apache.hadoop.hdfs.server.datanode.BlockReceiver.BlockReceiver}
 * @author taojiaen
 *
 */
public class BlockReciverDecoder {
	public BlockReciverDecoder(final ExtendedBlock block, final StorageType storageType,
		      final Channel in,
		      final String inAddr, final String myAddr,
		      final BlockConstructionStage stage,
		      final long newGs, final long minBytesRcvd, final long maxBytesRcvd,
		      final String clientname, final DatanodeInfo srcDataNode,
		      final DataNode datanode, DataChecksum requestedChecksum,
		      CachingStrategy cachingStrategy,
		      final boolean allowLazyPersist,
		      final boolean pinning) {

	}
}
