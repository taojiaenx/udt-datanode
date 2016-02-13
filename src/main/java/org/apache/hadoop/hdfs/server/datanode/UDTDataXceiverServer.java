package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.PeerServer;

/**
 * 以udt模式启动的数据节点服务器
 * @author taojiaen
 *
 */
class UDTDataXceiverServer extends DataXceiverServer{

	UDTDataXceiverServer(PeerServer peerServer, Configuration conf,
			DataNode datanode) {
		super(peerServer, conf, datanode);
	}
	@Override
	  public void run() {}

}
