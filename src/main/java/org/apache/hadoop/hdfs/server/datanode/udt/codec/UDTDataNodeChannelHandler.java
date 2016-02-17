package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import io.netty.util.AttributeKey;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;

public interface UDTDataNodeChannelHandler {

	static AttributeKey<UDTChannelMode> CHANNEL_MODE = AttributeKey.valueOf("UDT_CHANNEL_MODE");
	static AttributeKey<Op> CHANNEL_OPERATION =  AttributeKey.valueOf("UDT_CHANNEL_OPERATION");
	static  int LENGTH_FIELD_OFFSET = 1;
	static int LENGTH_FIELD_LENGETH = 5;
}
