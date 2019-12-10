package com.sbk.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.io.IOException
import kotlin.test.assertFailsWith


class FileSystemDeleteTest {

    private var fs = FileSystem.get(Configuration())
    private var DIR_FILE = "dir/file"
    private var DIR = "dir"

    @Before
    fun setUp() {
        writeFile(fs, Path(DIR_FILE))
    }

    private fun writeFile(fileSys: FileSystem, name: Path) {
        val stm = fileSys.create(name)
        stm.close()
    }

    @Test
    fun deleteFile() {
        Assert.assertTrue(fs.delete(Path(DIR_FILE), false))
        Assert.assertFalse(fs.exists(Path(DIR_FILE)))
        Assert.assertTrue(fs.exists(Path(DIR)))
        Assert.assertTrue(fs.delete(Path(DIR), false))
        Assert.assertFalse(fs.exists(Path(DIR)))
    }

    @Test
    fun deleteNonEmptyDirectoryNonRecursivelyFails() {
        assertFailsWith<IOException> { fs.delete(Path(DIR), false) }
    }

    @Test
    fun deleteDirectory() {
        Assert.assertTrue(fs.delete(Path(DIR), true))
        Assert.assertFalse(fs.exists(Path(DIR)))
    }
}