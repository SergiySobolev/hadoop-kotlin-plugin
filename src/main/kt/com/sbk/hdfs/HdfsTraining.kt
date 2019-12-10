package com.sbk.hdfs

class HdfsTraining {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            var quit = false
            while (!quit) {
                when (readLine()?.toInt() ?: 0) {
                    0 -> quit = true
                    1 -> URLCat(Constants.QUANGLE_FILE_URI).copyBytes()
                    2 -> FileSystemCat(Constants.QUANGLE_FILE_URI).copyBytes()
                    3 -> FileSystemDoubleCat(Constants.QUANGLE_FILE_URI).copyBytes()
                    4 -> ListStatus().show()
                    5 -> FileCopyWithProgress().copyTo()
                    else -> print("No such option")
                }
                print("\n")
            }
        }
    }
}