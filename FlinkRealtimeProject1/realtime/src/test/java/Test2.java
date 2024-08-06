import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSON;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/06 20:56
 * @contact: yuanxin9997@qq.com
 * @description: 从map对象中删除元素，必须要使用迭代器的方式，不能使用遍历的方式
 */
public class Test2 {
    public static void main(String[] Args) {
//        JSONObject data = JSON.parseObject("{}");
        JSONObject data = new JSONObject();
        data.put("a", 10);
        data.put("b", 20);
        data.put("c", 30);
        List<String> columns = Arrays.asList("a,c".split(","));

        //        while (it.hasNext()) {
//            String key = it.next();
//            if (!columns.contains(key)) {
//                it.remove();
//            }
//        }
        data.keySet().removeIf(key -> !columns.contains(key));
        System.out.println(data);
    }
}
