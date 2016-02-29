package org.apache.hadoop.hdfs.net;

import java.io.IOException;
import java.net.Socket;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.net.StandardSocketFactory;

import com.barchart.udt.nio.SelectorProviderUDT;
import com.barchart.udt.nio.SocketChannelUDT;

/**
 * Specialized UDTSocketFactory to create sockets with a SOCKS proxy
 * String org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT
 * = "org.apache.hadoop.net.StandardSocketFactory
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UdtThreadFactory extends StandardSocketFactory{
	 @Override
	  public Socket createSocket() throws IOException {
	    /*
	     * NOTE: This returns an NIO socket so that it has an associated
	     * SocketChannel. As of now, this unfortunately makes streams returned
	     * by Socket.getInputStream() and Socket.getOutputStream() unusable
	     * (because a blocking read on input stream blocks write on output stream
	     * and vice versa).
	     *
	     * So users of these socket factories should use
	     * NetUtils.getInputStream(socket) and
	     * NetUtils.getOutputStream(socket) instead.
	     *
	     * A solution for hiding from this from user is to write a
	     * 'FilterSocket' on the lines of FilterInputStream and extend it by
	     * overriding getInputStream() and getOutputStream().
	     */
	    return SelectorProviderUDT.STREAM.openSocketChannel().socket();
	  }
}
