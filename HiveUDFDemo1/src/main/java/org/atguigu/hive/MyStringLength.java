package org.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Hive 用户自定义方法类demo
 */
public class MyStringLength extends GenericUDF {
    /**
     * 初始化方法：检查函数参数个数类型，并返回函数的的返回值的类型检查器
     * @param arguments
     * @return 函数的的返回值的类型检查器
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 1. 判断参数的个数
        if(arguments.length != 1){
            throw new UDFArgumentLengthException("需要输入1个参数");
        }
        // 2. 判断参数的类型
        if(!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0, "需要输入正确类型");
        }
        // 3. 返回函数返回值的类型检查器
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的核心的代码：判断长度
     * @param arguments
     * @return 返回参数的长度
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 1. null值的问题
        if(arguments[0].get()== null){
            return 0;
        }
        // 2. 返回参数的长度
        return arguments[0].get().toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
