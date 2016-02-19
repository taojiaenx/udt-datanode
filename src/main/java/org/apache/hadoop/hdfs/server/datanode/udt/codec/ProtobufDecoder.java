package org.apache.hadoop.hdfs.server.datanode.udt.codec;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;

/**
 * protoBuffer解析器
 * @author taojiaen
 *
 */
public class ProtobufDecoder {
	private static final boolean HAS_PARSER;
	static {
        boolean hasParser = false;
        try {
            // MessageLite.getParsetForType() is not available until protobuf 2.5.0.
            MessageLite.class.getDeclaredMethod("getParserForType");
            hasParser = true;
        } catch (Throwable t) {
            // Ignore
        }

        HAS_PARSER = hasParser;
    }
    private MessageLite prototype;
    private final ExtensionRegistry extensionRegistry = null;


    /**
     * 解析protoBuf
     * @param in
     * @return
     * @throws IOException
     */
	public Object decode(ByteBuf in) throws IOException {
		final ByteBuf msg = vintPrefixed(in);

		if (msg != null) {
			try {
				return decodeMessage(msg);
			} finally {
				ReferenceCountUtil.release(msg);
			}
		}
		return null;

	}




    public MessageLite getPrototype() {
		return prototype;
	}




	public void setPrototype(MessageLite prototype) {
		this.prototype = prototype;
	}




	/**
	 * 生成长度可变的protobuf数据包长度
	 * @param in
	 * @return
     * @throws IOException
	 */
	private static ByteBuf vintPrefixed(final ByteBuf in) throws IOException {
		in.markReaderIndex();
		final byte[] buf = new byte[5];
		for (int i = 0; i < buf.length; i++) {
			if (!in.isReadable()) {
				in.resetReaderIndex();
				return null;
			}

			buf[i] = in.readByte();
			if (buf[i] >= 0) {
				int length = CodedInputStream.newInstance(buf, 0, i + 1).readRawVarint32();
				if (length < 0) {
					throw new CorruptedFrameException("negative length: " + length);
				}

				if (in.readableBytes() < length) {
					in.resetReaderIndex();
					return null;
				} else {
					return in.readBytes(length);
				}
			}
		}

		// Couldn't find the byte whose MSB is off.
		throw new CorruptedFrameException("length wider than 32-bit");
	}

    /**
     * 解析protobuffer解析
     * @param msg
     * @return
     * @throws InvalidProtocolBufferException
     */
    private Object decodeMessage(ByteBuf msg) throws InvalidProtocolBufferException {
    	final byte[] array;
        final int offset;
        final int length = msg.readableBytes();
        if (msg.hasArray()) {
            array = msg.array();
            offset = msg.arrayOffset() + msg.readerIndex();
        } else {
            array = new byte[length];
            msg.getBytes(msg.readerIndex(), array, 0, length);
            offset = 0;
        }

        if (extensionRegistry == null) {
            if (HAS_PARSER) {
                return prototype.getParserForType().parseFrom(array, offset, length);
            } else {
                return prototype.newBuilderForType().mergeFrom(array, offset, length).build();
            }
        } else {
            if (HAS_PARSER) {
                return prototype.getParserForType().parseFrom(array, offset, length, extensionRegistry);
            } else {
                return prototype.newBuilderForType().mergeFrom(array, offset, length, extensionRegistry).build();
            }
        }
    }
}
