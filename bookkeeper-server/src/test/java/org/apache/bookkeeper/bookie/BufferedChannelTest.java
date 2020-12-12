package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(Enclosed.class)
public class BufferedChannelTest {

    @RunWith(Parameterized.class)
    public static class ReadTest {

        ByteBuf writeBuffer;
        int writeCapacity;
        long pos;
        int length;
        static BufferedChannel bufferedChannel;
        Object result;

        public ReadTest(int writeCapacity, ByteBuf writeBuffer, long pos, int length, Object result) {
            this.writeBuffer = writeBuffer;
            this.writeCapacity = writeCapacity;
            this.pos = pos;
            this.length = length;
            this.result = result;
        }

        @Parameterized.Parameters
        public static Collection readParameters() {

            return Arrays.asList(new Object[][]{

                    //WriteCapacity, ByteBuf, pos, length, result

                    //Minimal Set
                    {0, null, 0, 0, NullPointerException.class},
                    //{1, Unpooled.buffer(0), 1, 1, 0}, //infinite loop
                    {1, Unpooled.buffer(1), 1, 1, 7}, //gives back 7 because there are 8 bytes written and 1 is skipped

                    {50, null, 0, 0, NullPointerException.class},
                    {50, Unpooled.buffer(100), 0, 10, IOException.class}, //Nel buffer ci sono solamente 4 scritture, quindi nella
                    //posizione 10 non c'è nulla.
                    {50, Unpooled.buffer(100), 0, 4, 8},
                    {50, Unpooled.buffer(100), -1, 0, 0},

            });

        }

        @Test
        public void readTest() {

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(writeCapacity, 50);

                //Writing 8 bytes
                for (int i = 0; i < 8; i++) {

                    writeBuffer.writeByte(1);

                }

                bufferedChannel.write(writeBuffer);
                Assert.assertEquals(result, bufferedChannel.read(writeBuffer, pos, length));

            } catch (Exception e) {

                Assert.assertEquals(e.getClass(), result);

            }

        }

        //Closing the BufferedChannel
        @AfterClass
        public static void close() throws IOException {

            try {

                bufferedChannel.close();

            } catch (NullPointerException e) {

                e.printStackTrace();
            }

        }

    }

    @RunWith(Parameterized.class)
    public static class WriteTest {

        int writeCapacity;
        long position;
        ByteBuf writeBuffer;
        int unpersistedBytesBound;
        static BufferedChannel bufferedChannel;
        Object result;

        public WriteTest(int writeCapacity, long position, ByteBuf writeBuffer, int unpersistedBytesBound, Object result) {
            this.writeCapacity = writeCapacity;
            this.position = position;
            this.writeBuffer = writeBuffer;
            this.unpersistedBytesBound = unpersistedBytesBound;
            this.result = result;
        }

        @Parameterized.Parameters
        public static Collection writeParameters() {

            return Arrays.asList(new Object[][]{

                    //WriteCapacity, Position, ByteBuf writeBuffer, UnpersistedBytesBound, ExpectedResult//

                    //Minimal Set
                    {50, 0, null, 0, NullPointerException.class},
                    {50, 0, Unpooled.buffer(0), 1, (long) 1},
                    {50, 0, Unpooled.buffer(1), 1, (long) 1},

                    //variazione su UnpersistedBytesBound impostato ad 1>0, quindi ci aspettiamo
                    //che la scrittura venga correttamente eseguita.
                    {50, 0, null, 1, NullPointerException.class},
                    {50, 0, Unpooled.buffer(0), 0, (long) 0},
                    {50, 0, Unpooled.buffer(1), 0, (long) 0},

                    //ulteriore test
                    {50, 0, Unpooled.buffer(1), 20, (long) 0}

            });

        }

        @Before
        public void openChannel() {

            try {

                File file = File.createTempFile("test", "");
                file.deleteOnExit();
                FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
                bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel, writeCapacity, 10, unpersistedBytesBound);

            } catch (Exception e) {

                e.printStackTrace();

            }

        }

        @After
        public void close() throws IOException {

            bufferedChannel.close();

        }


        @Test
        public void writeTest() {

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(writeCapacity, unpersistedBytesBound);
                writeBuffer.writeByte(10);
                bufferedChannel.write(writeBuffer);

                Assert.assertEquals((long) result, bufferedChannel.fileChannel.size());

            } catch (Exception e) {

                Assert.assertEquals(e.getClass(), result);

            }

        }
    }

    public static class CoverageTest {

        @Test
        public void positionTest() {


            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 20);

                ByteBuf byteBuf = Unpooled.buffer(25);
                byte[] bytes = new byte[5];
                for (int i = 0; i < bytes.length; i++) {

                    bytes[i] = (byte) i;
                }
                byteBuf.writeBytes(bytes);

                Assert.assertEquals(0, bufferedChannel.position());

                bufferedChannel.write(byteBuf);
                Assert.assertEquals(bytes.length, bufferedChannel.position());

            } catch (IOException e) {

                e.printStackTrace();
            }

        }

        @Test
        public void getFileChannelPositionTest() {

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 1);

                ByteBuf byteBuf = Unpooled.buffer(25);
                byteBuf.writeByte(1);

                Assert.assertEquals(0, bufferedChannel.getFileChannelPosition());
                bufferedChannel.write(byteBuf);
                Assert.assertEquals(1, bufferedChannel.getFileChannelPosition());

            } catch (IOException e) {

                e.printStackTrace();
            }
        }

        @Test
        public void flushTest() {

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 20);

                ByteBuf byteBuf = Unpooled.buffer(25);
                byte[] bytes = new byte[5];
                for (int i = 0; i < bytes.length; i++) {

                    bytes[i] = (byte) i;
                }
                byteBuf.writeBytes(bytes);

                bufferedChannel.write(byteBuf);
                Assert.assertEquals(0, bufferedChannel.fileChannel.size());
                bufferedChannel.flush();
                Assert.assertEquals(5, bufferedChannel.fileChannel.size());

            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        @Test
        public void forceWriteTest() {

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 1);

                ByteBuf byteBuf = Unpooled.buffer(25);
                byte[] bytes = new byte[5];
                for (int i = 0; i < bytes.length; i++) {

                    bytes[i] = (byte) i;
                }
                byteBuf.writeBytes(bytes);

                bufferedChannel.write(byteBuf);
                Assert.assertEquals(5, bufferedChannel.forceWrite(true));

                bufferedChannel.write(byteBuf);
                Assert.assertEquals(10, bufferedChannel.forceWrite(true));

                BufferedChannel bufferedChannel1 = createBufferedChannel(20, 0);
                ByteBuf byteBuf1 = Unpooled.buffer(25);

                byte[] bytes1 = new byte[5];
                for (int i = 0; i < bytes1.length; i++) {

                    bytes1[i] = (byte) i;
                }
                byteBuf1.writeBytes(bytes1);

                bufferedChannel1.write(byteBuf1);
                Assert.assertEquals(0, bufferedChannel1.forceWrite(true));

            } catch (IOException e) {

                e.printStackTrace();
            }


        }

        @Test
        public void flushAndForceWriteIfRegularFlushTest() {

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 1);

                ByteBuf byteBuf = Unpooled.buffer(25);
                byte[] bytes = new byte[5];
                for (int i = 0; i < bytes.length; i++) {

                    bytes[i] = (byte) i;
                }
                byteBuf.writeBytes(bytes);

                bufferedChannel.write(byteBuf);

                bufferedChannel.flushAndForceWriteIfRegularFlush(true);
                Assert.assertEquals(5, bufferedChannel.forceWrite(true));

            } catch (IOException e) {

                e.printStackTrace();
            }

        }

        @Test
        public void clearTest() {

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 10);

                ByteBuf byteBuf = Unpooled.buffer(15);
                byte[] bytes = new byte[5];
                for (int i = 0; i < bytes.length; i++) {

                    bytes[i] = (byte) i;
                }
                byteBuf.writeBytes(bytes);

                bufferedChannel.write(byteBuf);

                Assert.assertEquals(5, byteBuf.writerIndex());
                bufferedChannel.clear();
                //Assert.assertEquals(0, byteBuf.writerIndex());

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Test
        public void getNumOfBytesInWriteBufferTest(){

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 10);

                ByteBuf byteBuf = Unpooled.buffer(15);
                byte[] bytes = new byte[5];
                for (int i = 0; i < bytes.length; i++) {

                    bytes[i] = (byte) i;
                }
                byteBuf.writeBytes(bytes);

                bufferedChannel.write(byteBuf);

                Assert.assertEquals(5, bufferedChannel.getNumOfBytesInWriteBuffer());

            } catch (IOException e) {

                e.printStackTrace();
            }


        }

        @Test
        public void getUnpersistedBytesTest(){

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 10);

                ByteBuf byteBuf = Unpooled.buffer(15);
                byte[] bytes = new byte[5];
                for (int i = 0; i < bytes.length; i++) {

                    bytes[i] = (byte) i;
                }
                byteBuf.writeBytes(bytes);

                bufferedChannel.write(byteBuf);

                Assert.assertEquals(5, bufferedChannel.getUnpersistedBytes());

            } catch (IOException e) {

                e.printStackTrace();
            }

        }

        @Test
        public void closeTest(){

            try {

                BufferedChannel bufferedChannel = createBufferedChannel(20, 1);

                Assert.assertTrue(bufferedChannel.fileChannel.isOpen());
                bufferedChannel.close();
                Assert.assertFalse(bufferedChannel.fileChannel.isOpen());

            } catch (IOException e) {


                e.printStackTrace();
            }

        }

    }

    public static BufferedChannel createBufferedChannel(int writeCapacity, int unpersistedBytesBound) throws IOException {

        ByteBufAllocator byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        File file = File.createTempFile("testFile", "");
        file.deleteOnExit();
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();

        BufferedChannel bufferedChannel = new BufferedChannel(byteBufAllocator, fileChannel, writeCapacity, 100, unpersistedBytesBound);

        return bufferedChannel;

    }

}
