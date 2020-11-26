package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.util.ZeroBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Enclosed.class)
public class ZeroBufferEnclosedTest {

    @RunWith(Parameterized.class)
    public static class PutTest{

        ByteBuffer dst;
        Integer length;
        Object resultPut;

        public PutTest(ByteBuffer dst, Integer length, Object resultPut) {
            this.dst = dst;
            this.length = length;
            this.resultPut = resultPut;
        }


        @Parameterized.Parameters
        public static Collection zeroBufferParametersPut(){

            //Bytebuffer, length, resultPut
            return Arrays.asList(new Object[][]{


                    //Minimal Test-Set
                    {null, 0, NullPointerException.class},
                    {createByteBuffer(0), 1, BufferOverflowException.class}, //This exception is raised because the buffer’s current position is greater than its limit
                    {createByteBuffer(1), 1, 1}


                    /*
                    {createByteBuffer(0), 0, 0},
                    {createByteBuffer(100), 34, 34},
                    {null, 0, NullPointerException.class},
                    {createByteBuffer(50), -1, 0},

                    //coverage
                    //length > zeroBytes
                    {createByteBuffer(67*1024), 65*1024, 65*1024},
                    {createByteBuffer(65*1024), 64*1024, 64*1024},
                    {null, -1, NullPointerException.class},
                    {createByteBuffer(100), 0, 0}
                     */

            });

        }
        @Test
        public void putTest(){

            try{

                ZeroBuffer.put(dst, length);
                Assert.assertEquals(resultPut, dst.position());

            }catch (Exception e){

                Assert.assertEquals(e.getClass(), resultPut);

            }

        }

    }

    @RunWith(Parameterized.class)
    public static class ReadTest{

        int length;
        Object resultReader;

        public ReadTest(int length, Object resultReader) {
            this.length = length;
            this.resultReader = resultReader;
        }

        @Parameterized.Parameters
        public static Collection zeroBufferParametersRead(){

            return Arrays.asList(new Object[][]{

                    //Minimal Test-Set
                    {0, 0},
                    {1, 1},


                    //coverage
                    //length > zerobytes.length
                    //{-1, IndexOutOfBoundsException.class},
                    //{65*1024,  65*1024},


            });

        }

        @Test
        public void readOnlyBufferTest(){

            try{

                ByteBuffer bBuffer = ZeroBuffer.readOnlyBuffer(length);
                Assert.assertEquals(resultReader, bBuffer.limit());

            }catch (Exception e){

                Assert.assertEquals(e.getClass(), resultReader);

            }

        }

    }

    @RunWith(Parameterized.class)
    public static class OtherPutTest{

        ByteBuffer dst;
        Object resultOtherPut;

        public OtherPutTest(ByteBuffer dst, Object resultOtherPut) {
            this.dst = dst;
            this.resultOtherPut = resultOtherPut;
        }

        @Parameterized.Parameters
        public static Collection otherPutParameters(){

            return Arrays.asList(new Object[][]{

                    //Minimal Test-Set
                    {null, NullPointerException.class},
                    {createByteBuffer(0), 0},
                    {createByteBuffer(1), 1}


                    //{ByteBuffer.allocate(50), 50}

            });

        }

        @Test
        public void otherPutTest(){

            try{

                ZeroBuffer.put(dst);

                Assert.assertEquals(resultOtherPut, dst.limit());

            }catch (Exception e){

                Assert.assertEquals(e.getClass(), resultOtherPut);

            }

        }

    }

    public static ByteBuffer createByteBuffer(int length){

        ByteBuffer bb = ByteBuffer.allocate(length);
        return bb;

    }

}
