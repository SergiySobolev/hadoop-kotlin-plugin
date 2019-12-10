import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.hamcrest.CoreMatchers
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.io.FileNotFoundException
import java.io.IOException

class ShowFileStatusTest {

    private val cluster = MiniDFSCluster.Builder(Configuration()).build()
    private var fs = cluster.fileSystem

    @Before
    @Throws(IOException::class)
    fun setUp() {
        System.setProperty("test.build.data", "/tmp")
        fs.create(Path("/dir/file"))
                .use { o -> o.write("content-".toByteArray(charset("UTF-8"))) }
    }

    @After
    @Throws(IOException::class)
    fun tearDown() {
        fs?.close()
        cluster?.shutdown()
    }

    @Test(expected = FileNotFoundException::class)
    @Throws(IOException::class)
    fun throwsFileNotFoundForNonExistentFile() {
        fs!!.getFileStatus(Path("no-such-file"))
    }

    @Test
    @Throws(IOException::class)
    fun fileStatusForFile() {
        val file = Path("/dir/file")
        val stat = fs!!.getFileStatus(file)
        Assert.assertEquals(stat.path.toUri().path, "/dir/file")
        Assert.assertFalse(stat.isDirectory)
        Assert.assertEquals(stat.len, 8L)
        Assert.assertEquals(stat.replication, 1.toShort()
        )
        Assert.assertEquals(stat.blockSize, 128 * 1024 * 1024L)
        Assert.assertEquals(stat.owner, System.getProperty("user.name"))
        Assert.assertEquals(stat.group, "supergroup")
        Assert.assertEquals(stat.permission.toString(), "rw-r--r--")
    }

    @Test
    @Throws(IOException::class)
    fun fileStatusForDirectory() {
        val dir = Path("/dir")
        val stat = fs!!.getFileStatus(dir)
        Assert.assertThat(stat.path.toUri().path, CoreMatchers.`is`("/dir"))
        Assert.assertThat(stat.isDirectory, CoreMatchers.`is`(true))
        Assert.assertThat(stat.len, CoreMatchers.`is`(0L))
        Assert.assertThat(stat.replication, CoreMatchers.`is`(0.toShort()))
        Assert.assertThat(stat.blockSize, CoreMatchers.`is`(0L))
        Assert.assertThat(stat.owner, CoreMatchers.`is`(System.getProperty("user.name")))
        Assert.assertThat(stat.group, CoreMatchers.`is`("supergroup"))
        Assert.assertThat(stat.permission.toString(), CoreMatchers.`is`("rwxr-xr-x"))
    }
}