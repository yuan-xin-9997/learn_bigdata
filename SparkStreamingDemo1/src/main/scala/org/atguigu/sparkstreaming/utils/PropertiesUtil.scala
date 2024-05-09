package org.atguigu.sparkstreaming.utils

import java.util.ResourceBundle

/**
 * Created by VULCAN on 2020/11/3
 */
object PropertiesUtil {

    // 绑定配置文件
    // ResourceBundle专门用于读取配置文件，所以读取时，不需要增加扩展名
    val resourceFile: ResourceBundle = ResourceBundle.getBundle("db")

    def getValue( key : String ): String = {
        resourceFile.getString(key)
    }


}
