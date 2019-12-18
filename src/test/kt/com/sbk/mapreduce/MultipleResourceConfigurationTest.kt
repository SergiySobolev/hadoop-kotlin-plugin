package com.sbk.mapreduce

import org.apache.hadoop.conf.Configuration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MultipleResourceConfigurationTest {
    @Test
    fun get() {
        val conf = Configuration()
        conf.addResource("configuration-1.xml")
        conf.addResource("configuration-2.xml")

        assertEquals(conf["color"], "yellow")
        assertEquals(conf.getInt("size", 0), 12)
        assertEquals(conf["weight"], "heavy")
        assertEquals(conf["size-weight"], "12,heavy")
        System.setProperty("size", "14")
        assertEquals(conf["size-weight"], "14,heavy")

        System.setProperty("length", "2")
        assertNull(conf["length"])
    }
}