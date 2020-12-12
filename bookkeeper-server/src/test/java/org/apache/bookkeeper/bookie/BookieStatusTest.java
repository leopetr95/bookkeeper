package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.BookieStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(Enclosed.class)
public class BookieStatusTest {

    @BeforeClass
    public static void createDirectory(){

        File file = new File("anExamplePath");

        try{

            file.mkdir();

        }catch (Exception e){

            e.printStackTrace();

        }

    }

    @AfterClass
    public static void deleteDirectory(){

        File file = new File("anExamplePath");

        try{

            String[] stringsFile = file.list();
            if (stringsFile != null) {
                for(String string: stringsFile){

                    File stringFile = new File(file.getPath(), string);
                    stringFile.delete();
                }
            }

            file.delete();

        }catch (Exception e){

            e.printStackTrace();
        }

    }

    @RunWith(Parameterized.class)
    public static class ParseTest{

        String layoutVersion;
        String bookieMode;
        long lastUpdateTime;

        public ParseTest(String layoutVersion, String bookieMode, long lastUpdateTime) {
            this.layoutVersion = layoutVersion;
            this.bookieMode = bookieMode;
            this.lastUpdateTime = lastUpdateTime;
        }

        @Parameterized.Parameters
        public static Collection ParseTestParameters(){

            return Arrays.asList(new Object[][]{

                    //LayoutVersion, BookieMode, LastUpdateTime
                    {"1", "READ_ONLY", System.currentTimeMillis()},
                    {"1", "READ_WRITE", System.currentTimeMillis()},

            });

        }

        @Test
        public void parseTest() throws IOException {

            BookieStatus bookieStatus = new BookieStatus();
            String string = layoutVersion.concat(",").concat(bookieMode).concat(",").concat(String.valueOf(lastUpdateTime));
            StringReader stringReader = new StringReader(string);
            BufferedReader bufferedReader = new BufferedReader(stringReader);

            Assert.assertEquals(BookieStatus.class, bookieStatus.parse(bufferedReader).getClass());

        }

        @Test
        public void parsingNullTest() throws IOException{

            BookieStatus bookieStatus = new BookieStatus();
            BufferedReader bufferedReader = Mockito.mock(BufferedReader.class);
            when(bufferedReader.readLine()).thenReturn(null);
            Assert.assertSame(null, bookieStatus.parse(bufferedReader));

        }

        @Test
        public void parsingEmptyTest() throws IOException{

            BookieStatus bookieStatus = new BookieStatus();
            BufferedReader bufferedReader = Mockito.mock(BufferedReader.class);
            when(bufferedReader.readLine()).thenReturn("");
            Assert.assertSame(null, bookieStatus.parse(bufferedReader));

        }

        @Test
        public void parsingIntegerExceptionTest() throws IOException{

            try{

                BookieStatus bookieStatus = new BookieStatus();
                BufferedReader bufferedReader = Mockito.mock(BufferedReader.class);
                when(bufferedReader.readLine()).thenReturn("testIncorrect");
                bookieStatus.parse(bufferedReader);

            }catch (Exception e){

                Assert.assertEquals(e.getClass(), NumberFormatException.class);
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class ReadAndWriteDirectoriesTest{

        String path;
        Object res;

        public ReadAndWriteDirectoriesTest(String path, Object res) {
            this.path = path;
            this.res = res;
        }

        @Parameterized.Parameters
        public static Collection readAndWriteDirectoriesParameters() throws Exception{

            return Arrays.asList(new Object[][]{

                    //Path, ResultExpected
                    {null, NullPointerException.class},
                    {"anExamplePath", null},
                    {"", IOException.class}
            });
        }

        @BeforeClass
        public static void createDirectory(){

            File file = new File("path");

            try{

                file.mkdir();

            }catch (Exception e){

                e.printStackTrace();
            }
        }

        @AfterClass
        public static void deleteDirectory(){

            File file = new File("path");
            try{

                String[] strings = file.list();
                if (strings != null) {
                    for(String string: strings){

                        File stringFile = new File(file.getPath(), string);
                        stringFile.delete();

                    }
                }

                file.delete();

            }catch (Exception e){

                e.printStackTrace();
            }
        }

        @Test
        public void readFromDirectoriesTest(){

            try{

                List<File> files = new ArrayList<>();
                File file = new File("path");
                files.add(file);
                new BookieStatus().readFromDirectories(files);

            }catch (Exception e){

                Assert.assertEquals(e.getClass(), res);
            }
        }

        @Test
        public void read1FromDirectoriesTest(){

            try{

                List<File> files = new ArrayList<>();

                new BookieStatus().readFromDirectories(files);

            }catch (Exception e){

                Assert.assertEquals(e.getClass(), res);
            }
        }

        @Test
        public void writeToDirectoriesTest(){

            try{

                List<File> files = new ArrayList<>();
                File file = new File("path");
                files.add(file);
                BookieStatus status = new BookieStatus();
                status.writeToDirectories(files);

            }catch (Exception e){

                Assert.assertEquals(e.getClass(), res);
            }
        }

        @Test
        public void writeToDirectories1Test(){

            try{

                List<File> files = new ArrayList<>();
                BookieStatus status = new BookieStatus();
                status.writeToDirectories(files);

            }catch (Exception e){

                Assert.assertEquals(e.getClass(), res);
            }



        }

    }

    public static class mutationCoverageTest{

        @Test
        public void toStringTest(){

            BookieStatus bookieStatus = new BookieStatus();
            Assert.assertNotEquals("", bookieStatus.toString());
        }

        @Test
        public void ifWritableTest(){

            BookieStatus bookieStatus = new BookieStatus();
            bookieStatus.setToReadOnlyMode();
            Assert.assertFalse(bookieStatus.isInWritable());
        }

        @Test
        public void writableAfterReadOnly(){

            BookieStatus bookieStatus = new BookieStatus();
            Assert.assertFalse(bookieStatus.isInReadOnlyMode());

            bookieStatus.setToReadOnlyMode();
            Assert.assertTrue(bookieStatus.isInReadOnlyMode());
        }

        @Test
        public void setWritableTest(){

            BookieStatus bookieStatus = new BookieStatus();
            //Bookie is already in Write Mode
            Assert.assertFalse(bookieStatus.setToWritableMode());

            //Setting Bookie into read mode only
            bookieStatus.setToReadOnlyMode();
            Assert.assertTrue(bookieStatus.setToWritableMode());
        }

        @Test
        public void setReadOnlyTest(){

            BookieStatus bookieStatus = new BookieStatus();
            Assert.assertTrue(bookieStatus.setToReadOnlyMode());

            Assert.assertFalse(bookieStatus.setToReadOnlyMode());
        }

    }

}
