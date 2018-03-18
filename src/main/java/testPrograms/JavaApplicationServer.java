package testPrograms;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;

import org.apache.commons.lang3.RandomUtils;

public class JavaApplicationServer {
    protected static void writeToSocket(ByteBuffer data, SocketChannel socketChannel)
            throws IOException {
        data.compact();
//        System.out.println(data.capacity() + " "+ data.position() +
//                " " + data.limit());
        data.flip();
//        System.out.println(data.capacity() + " "+ data.position() +
//                " " + data.limit());
        while (data.hasRemaining()) {
            socketChannel.write(data);
        }
        data.clear();
    }

    public static void main(String[] args) throws Exception{
        Charset charset = Charset.forName("ISO-8859-1");
        ServerSocketChannel s = ServerSocketChannel.open();
        s.configureBlocking(true);
        s.socket().bind(new InetSocketAddress(10024));

        SocketChannel sc = s.accept();

        String key = "123";
        short flags = 1;
        int bytes = 10;
        final byte[] data = RandomUtils.nextBytes(bytes);

        CharBuffer charBuf = CharBuffer.wrap("set " + key + " " +
                flags + " " + bytes + "\r\n");
        ByteBuffer metadata = charset.encode(charBuf);
        System.out.println("Writing metadata to server: " +
                new String(metadata.array()));
        writeToSocket(metadata, sc);

        
        
        
/*        CharBuffer c = CharBuffer.wrap("Hello from server!");
        System.out.println("writing: " + c);
        ByteBuffer b = charset.encode(c);
        System.out.println("encoded string: " + new String(b.array()));

        //sc.configureBlocking(true);
        b.compact();
        System.out.println(b.capacity() + " "+ b.position() + " " + b.limit());
        b.flip();
        System.out.println(b.capacity() + " "+ b.position() + " " + b.limit());
        int a = 0;
        while (b.hasRemaining())
        {
            a += sc.write(b);
        }
*/
        sc.close();
        s.close();
//        System.out.println("wrote " + a);
        }
}
