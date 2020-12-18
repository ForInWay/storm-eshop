package com.morgan.storm.utils;

import java.text.MessageFormat;
import java.util.*;

/**
 * 工具类处理
 * @author Andy
 */
public class Tools
{
    /**
     * 随机数
     */
    private static final Random RANDOM = new Random();
    
    /**
     * 所有字符
     */
    private static final String CHARINFO = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
    
    /**
     * 获得32位唯一序列号
     * @return 32为ID字符串
     */
    public static String getUUID()
    {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
    
    /**
     * 生成指定长度的验证码
     * @param length 指定短信验证码长度
     * @return 短信验证码 170027 格式
     */
    public static String getRandomNum(Integer length)
    {
        StringBuffer randomNum = new StringBuffer();
        while (true)
        {
            if (randomNum.length() >= length)
            {
                break;
            }
            
            randomNum.append(RANDOM.nextInt(10));
        }
        return randomNum.toString();
    }
    
    /**
     * 随机生成字母和数字字符串
     * @param length 指定长度
     * @return 
     */
    public static String getRandom(Integer length)
    {
        StringBuffer randomNum = new StringBuffer();
        while (true)
        {
            if (randomNum.length() >= length)
            {
                break;
            }
            
            randomNum.append(CHARINFO.charAt(RANDOM.nextInt(CHARINFO.length())));
        }
        return randomNum.toString();
    }
    
    /**
     * 截取字符串长度
     * @param str
     * @param cutLeng
     * @return 截取后的字符串
     */
    public static String subStr(String str, int cutLeng)
    {
        if (str.length() > cutLeng)
        {
            return str.substring(0, cutLeng) + "...";
        }
        return str;
    }
    
    /**
     * 替换字符串中的占位符
     * @param msg 消息
     * @param args 占位符替换数组
     * @return
     */
    public static String getPlaceHolder(String msg, Object[] args)
    {
        return MessageFormat.format(msg, args);
    }
    
    /**
     * 判断list是否为空
     * @param list
     * @return true==为空
     */
    public static <T> boolean isEmpty(List<T> list)
    {
        return (list == null || list.size() == 0);
    }
    
    /**
     * 判断Map是否为空
     * @param map
     * @return true==为空
     */
    public static <K, V> boolean isEmpty(Map<K, V> map)
    {
        return (map == null || map.size() == 0);
    }
    
    /**
     * 判断Set是否为空
     * @param set
     * @return true==为空
     */
    public static <T> boolean isEmpty(Set<T> set)
    {
        return (set == null) || (set.size() == 0);
    }
    
    /**
     * 判断String是否为空
     * @param str
     * @return true==为空
     */
    public static boolean isEmpty(String str)
    {
        return (str == null || str.trim().length() == 0);
    }
    
    /**
     * 判断Object是否为空
     * @param obj
     * @return true==为空
     */
    public static <T> boolean isEmpty(T t)
    {
        return (t == null);
    }
    
    /**
     * 判断Object[]是否为空
     * @param arr
     * @return true==为空
     */
    public static <T> boolean isEmpty(T[] arr)
    {
        return (arr == null || arr.length == 0);
    }
    
    /**
     * 判断List不为空
     * @param list
     * @return true==不为空
     */
    public static <T> boolean isNotEmpty(List<T> list)
    {
        return !isEmpty(list);
    }
    
    /**
     * 判断Map不为空
     * @param map
     * @return true==不为空
     */
    public static <K, V> boolean isNotEmpty(Map<K, V> map)
    {
        return !isEmpty(map);
    }
    
    /**
     * 判断Set不为空
     * @param set
     * @return true==不为空
     */
    public static <T> boolean isNotEmpty(Set<T> set)
    {
        return !isEmpty(set);
    }
    
    /**
     * 判断String不为空
     * @param str
     * @return true==不为空
     */
    public static boolean isNotEmpty(String str)
    {
        return !isEmpty(str);
    }
    
    /**
     * 判断Object不为空
     * @param obj
     * @return true==不为空
     */
    public static <T> boolean isNotEmpty(T t)
    {
        return !isEmpty(t);
    }
    
    /**
     * 判断Object[]不为空
     * @param arrObject
     * @return true==不为空
     */
    public static <T> boolean isNotEmpty(T[] arrObject)
    {
        return !isEmpty(arrObject);
    }
    
    /**
     * 两个字符串不相等 返回 true
     * @param first
     * @param secend
     * @return
     */
    public static boolean notEquals(String first, String secend)
    {
        return !first.equals(secend);
    }
    
    /**
     * 两个字符串不相等 返回 true
     * @param first
     * @param secend
     * @return
     */
    public static boolean notEquals(Long first, Long secend)
    {
        return !first.equals(secend);
    }
    
    /**
     * 两个字符串不相等 返回 true
     * @param first
     * @param secend
     * @return
     */
    public static boolean notEquals(Integer first, Integer secend)
    {
        return !first.equals(secend);
    }
    
    /**
     * 根据传入的map判断是否返回新的hashmap，处理Map为空的情况
     */
    public static <K, V> Map<K, V> isGetNewMap(Map<K, V> map)
    {
        return Tools.isEmpty(map) ? new HashMap<K, V>() : map;
    }
    
    /**
     * 获取HashMap
     * @return
     */
    public static <K, V> Map<K, V> newHashMap()
    {
        return new HashMap<K, V>();
    }
    
    /**
     * 是否获取空hashset，判断为空情况
     * @param set
     * @return
     */
    public static <T> Set<T> isGetNewSet(Set<T> set)
    {
        return Tools.isEmpty(set) ? new HashSet<T>() : set;
    }
    
    /**
     * 获取HashSet
     * @return
     */
    public static <T> Set<T> newHashSet()
    {
        return new HashSet<T>();
    }
    
    /**
     * 获取HashSet
     * @return
     */
    public static <T> Set<T> newHashSet(Collection<T> t)
    {
        return new HashSet<T>(t);
    }
    
    /**
     * 是否获取新ArrayList
     * @param list
     * @return
     */
    public static <T> List<T> isGetNewList(List<T> list)
    {
        return Tools.isEmpty(list) ? new ArrayList<T>() : list;
    }
    
    /**
     * 获取ArrayList
     * @return
     */
    public static <T> List<T> newArrayList()
    {
        return new ArrayList<T>();
    }
    
    /**
     * 获取ArrayList
     * @return
     */
    public static <T> List<T> newArrayList(Collection<T> t)
    {
        return new ArrayList<T>(t);
    }
    
    /**
     * 获取LinkedHashMap
     * @return
     */
    public static <K, V> Map<K, V> newLinkedHashMap()
    {
        return new LinkedHashMap<K, V>();
    }
    
    /**
     * 获取list大小
     * @param list
     * @return
     */
    public static <T> Integer getListSize(List<T> list)
    {
        return null == list ? 0 : list.size();
    }
    
    /**
     * 获取set大小
     * @param set
     * @return
     */
    public static <T> Integer getSetSize(Set<T> set)
    {
        return null == set ? 0 : set.size();
    }
    
    /**
     * 获取map大小
     * @param map
     * @return
     */
    public static <K, V> Integer getMapSize(Map<K, V> map)
    {
        return null == map ? 0 : map.size();
    }
    
    /**
     * 转化String型空值 null为 ""
     * @param value
     * @return
     */
    public static String transStringNullValue(String value)
    {
        return null == value ? "" : value;
    }
    
    /**
     * 转化String型空值 null为 ""
     * @param value
     * @return
     */
    public static String transStringNullValue(Object value)
    {
        return null == value ? "" : value.toString();
    }
    
    /**
     * 转化Long型空值 null为 0L
     * @param value
     * @return
     */
    public static Long transLongNullValue(Long value)
    {
        return null == value ? 0L : value;
    }
    
    /**
     * 转化Double型空值 null为 0D
     * @param value
     * @return
     */
    public static Double transDoubleNullValue(Double value)
    {
        return null == value ? 0D : value;
    }
    
    /**
     * 转化Integer型空值 null为 0
     * @param value
     * @return
     */
    public static Integer transIntegerNullValue(Integer value)
    {
        return null == value ? 0 : value;
    }
}