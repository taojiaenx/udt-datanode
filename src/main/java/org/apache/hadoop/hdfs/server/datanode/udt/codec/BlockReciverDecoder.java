package org.apache.hadoop.hdfs.server.datanode.udt.codec;


import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;

/**
 * 以异步非阻塞形式工作的BlockReceiver{@link org.apache.hadoop.hdfs.server.datanode.BlockReceiver.BlockReceiver}
 * @author taojiaen
 *
 */
public class BlockReciverDecoder extends SimpleChannelInboundHandler<HttpRequest> {
	private boolean isDatanode;
	private boolean isClient;
	private long restartBudget;
	private long datanodeSlowLogThresholdMs;
	private long responseInterval;
	private boolean isTransfer;
	private final ReplicaInPipelineInterface replicaInfo;

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
		 try{
		      this.isDatanode = clientname.length() == 0;
		      this.isClient = !this.isDatanode;
		      this.restartBudget = datanode.getDnConf().restartReplicaExpiry;
		      this.datanodeSlowLogThresholdMs = datanode.getDnConf().datanodeSlowIoWarningThresholdMs;
		      // For replaceBlock() calls response should be sent to avoid socketTimeout
		      // at clients. So sending with the interval of 0.5 * socketTimeout
		      this.responseInterval = (long) (datanode.getDnConf().socketTimeout * 0.5);
		      this.isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
		          || stage == BlockConstructionStage.TRANSFER_FINALIZED;

		      if (LOG.isDebugEnabled()) {
		        LOG.debug(getClass().getSimpleName() + ": " + block
		            + "\n  isClient  =" + isClient + ", clientname=" + clientname
		            + "\n  isDatanode=" + isDatanode + ", srcDataNode=" + srcDataNode
		            + "\n  inAddr=" + inAddr + ", myAddr=" + myAddr
		            + "\n  cachingStrategy = " + cachingStrategy
		            + "\n  pinning=" + pinning
		            );
		      }

		      Object replicaHandler;
			//
		      // Open local disk out
		      //
		      if (isDatanode) { //replication or move
		        replicaHandler = datanode.data.createTemporary(storageType, block);
		      } else {
		        switch (stage) {
		        case PIPELINE_SETUP_CREATE:
		          replicaHandler = datanode.data.createRbw(storageType, block, allowLazyPersist);
		          datanode.notifyNamenodeReceivingBlock(
		              block, replicaHandler.getReplica().getStorageUuid());
		          break;
		        case PIPELINE_SETUP_STREAMING_RECOVERY:
		          replicaHandler = datanode.data.recoverRbw(
		              block, newGs, minBytesRcvd, maxBytesRcvd);
		          block.setGenerationStamp(newGs);
		          break;
		        case PIPELINE_SETUP_APPEND:
		          replicaHandler = datanode.data.append(block, newGs, minBytesRcvd);
		          block.setGenerationStamp(newGs);
		          datanode.notifyNamenodeReceivingBlock(
		              block, replicaHandler.getReplica().getStorageUuid());
		          break;
		        case PIPELINE_SETUP_APPEND_RECOVERY:
		          replicaHandler = datanode.data.recoverAppend(block, newGs, minBytesRcvd);
		          block.setGenerationStamp(newGs);
		          datanode.notifyNamenodeReceivingBlock(
		              block, replicaHandler.getReplica().getStorageUuid());
		          break;
		        case TRANSFER_RBW:
		        case TRANSFER_FINALIZED:
		          // this is a transfer destination
		          replicaHandler =
		              datanode.data.createTemporary(storageType, block);
		          break;
		        default: throw new IOException("Unsupported stage " + stage +
		              " while receiving block " + block + " from " + inAddr);
		        }
		      }
		      replicaInfo = replicaHandler.getReplica();
		      this.dropCacheBehindWrites = (cachingStrategy.getDropBehind() == null) ?
		        datanode.getDnConf().dropCacheBehindWrites :
		          cachingStrategy.getDropBehind();
		      this.syncBehindWrites = datanode.getDnConf().syncBehindWrites;
		      this.syncBehindWritesInBackground = datanode.getDnConf().
		          syncBehindWritesInBackground;

		      final boolean isCreate = isDatanode || isTransfer
		          || stage == BlockConstructionStage.PIPELINE_SETUP_CREATE;
		      streams = replicaInfo.createStreams(isCreate, requestedChecksum);
		      assert streams != null : "null streams!";

		      // read checksum meta information
		      this.clientChecksum = requestedChecksum;
		      this.diskChecksum = streams.getChecksum();
		      this.needsChecksumTranslation = !clientChecksum.equals(diskChecksum);
		      this.bytesPerChecksum = diskChecksum.getBytesPerChecksum();
		      this.checksumSize = diskChecksum.getChecksumSize();

		      this.out = streams.getDataOut();
		      if (out instanceof FileOutputStream) {
		        this.outFd = ((FileOutputStream)out).getFD();
		      } else {
		        LOG.warn("Could not get file descriptor for outputstream of class " +
		            out.getClass());
		      }
		      this.checksumOut = new DataOutputStream(new BufferedOutputStream(
		          streams.getChecksumOut(), HdfsConstants.SMALL_BUFFER_SIZE));
		      // write data chunk header if creating a new replica
		      if (isCreate) {
		        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum);
		      }
		    } catch (ReplicaAlreadyExistsException bae) {
		      throw bae;
		    } catch (ReplicaNotFoundException bne) {
		      throw bne;
		    } catch(IOException ioe) {
		      //IOUtils.closeStream(this);
		      cleanupBlock();

		      // check if there is a disk error
		      IOException cause = DatanodeUtil.getCauseIfDiskError(ioe);
		      DataNode.LOG.warn("IOException in BlockReceiver constructor. Cause is ",
		          cause);

		      if (cause != null) { // possible disk error
		        ioe = cause;
		        datanode.checkDiskErrorAsync();
		      }

		      throw ioe;
		    }
	}






	@Override
	protected void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {
		// TODO Auto-generated method stub

	}

	public String getStorageUuid() {
		return replicaInfo.getStorageUuid();
	  }
	/**
	 * 进行blockreceive 任务
	 * @author taojiaen
	 *
	 */
	class BlockRceiveTask implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}

	}
}

