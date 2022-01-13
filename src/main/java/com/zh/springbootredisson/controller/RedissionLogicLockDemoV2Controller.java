package com.zh.springbootredisson.controller;

import com.alibaba.fastjson.JSON;
import com.zh.springbootredisson.config.Goods;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.redisson.api.RScript;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 7
 * / \
 * 5   6
 * /\  /\
 * 1 2 3 4
 * <p>
 * <p>
 * 锁使用redis SET 结构 获取到的锁 包含 节点 和锁的类型
 * 使用 key value 实现锁
 * 锁的含义体现在key 上
 * 多元操作
 * orgId_1_add  1 加锁
 * orgId_5_delete 5 删除锁
 *
 * 锁的信息包含  三种  1、add   2、delete    3、move
 */
@RestController
@Api(value = "2.lock")
@RequestMapping("v2")
public class RedissionLogicLockDemoV2Controller {

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private RedisTemplate redisTemplate;


    /**
     * 1 节点新增 1_1
     * <p>
     * 1.1 祖先路径无锁、祖先路径有add 锁 不应  add_7： 节点正在新增
     * 1.2 祖先路径有delete 锁，返回异常信息 某个节点正在删除，请稍后再试  delete_5 ： 节点删除
     * 1.2 祖先有移动move 锁，移动 不允许新增 返回异常信息  5_move_6  ：节点5 移动到 6
     */
    @GetMapping("add")
    @ApiOperation(value = "1.文件新增", notes = "测试redis写入")
    @ResponseBody
    public String add(@RequestParam(defaultValue = "001") String userID, String id, String fileId) {

        redisTemplate.boundValueOps("StringKey").set("StringValue");

        //0  查看祖先路径有没有删除锁  移动锁 出入

        List<String> lockcheck = new ArrayList<>();
        lockcheck.add("delete_" + id);  //orgId_1_delete
        lockcheck.add("delete_" + "5"); //orgId_5_delete
        lockcheck.add("delete_" + "7"); //orgId_7_delete
        lockcheck.add("move_" + id);  // ordId_X_move_1
        lockcheck.add("move_" + "5"); // ordId_X_move_5
        lockcheck.add("move_" + "7"); // ordId_X_move_7
        lockcheck.add(id + "_move");  //移除  // ordId_1_move_X
        lockcheck.add("5" + "_move"); // ordId_5_move_X
        lockcheck.add("7" + "_move"); // ordId_7_move_X

        List<Object> keys = new ArrayList<>();
        keys.add("001_*");
        Object[] args = new Object[1];
        args[0] = "001_*";

        redissonClient.getBucket("foo").set("bar");
        String r = redissonClient.getScript().eval(RScript.Mode.READ_ONLY,
                "return redis.call('get', 'foo')", RScript.ReturnType.VALUE);

        String lua="local keys = redis.call('keys', 'foo');\n" +
                "--下面通过mget获取所有key的值，call后面是可变长参数，unpack是将数组变成可变长参数\n" +
                "local values = redis.call('mget', unpack(keys));\n" +
                "--定义返回结果\n" +
                "local keyValuePairs = {};" +
                " --#keys是获取keys的长度，这里作了个for循环，将key与value对应起来\n" +
                "--lua中字符串拼接是用..\n" +
                "for i = 1, #keys do\n" +
                "keyValuePairs[i] = keys[i] .. '\\t' .. values[i]\n" +
                "end;\n" +
                "--返回结果\n" +
                "return keyValuePairs;";

        redissonClient.getBucket("002_7_add").set("00001");
        RScript script = redissonClient.getScript(StringCodec.INSTANCE);


        List<String> rs = script.eval(RScript.Mode.READ_ONLY,
                lua, RScript.ReturnType.STATUS);

        Boolean c = script.eval(RScript.Mode.READ_ONLY,
                "return redis.call('keys', 'foo')", RScript.ReturnType.BOOLEAN);

        System.out.println(r);
        RScript rScript = redissonClient.getScript();
       // List<String> a = rScript.eval(RScript.Mode.READ_ONLY,"return redis.call('keys','*')",RScript.ReturnType.VALUE);
        List<String> entity = redissonClient.getScript().eval(RScript.Mode.READ_ONLY, "return redis.call('KEYS', 'KEYS[1]')", RScript.ReturnType.VALUE, keys, args);

        System.out.println(entity.size());
        /*RSet<String> set = redissonClient.getSet(userID);
        Set<String> setCopy=new HashSet<>(set.size());
        setCopy.addAll(set);
        List<String> locks1=new ArrayList<>();
        for(String lockString: lockcheck){
         locks1 = setCopy.stream().filter(o->o.startsWith(lockString)).collect(Collectors.toList());
        }

        if(locks1.size()>0){
            System.out.println("发现并发锁1");
            return  "发现锁： "+ setCopy.stream().findFirst().get();
        }
        setCopy.retainAll(lockcheck);
        if (setCopy.size() > 0) {
            System.out.println("发现并发锁2");
            // todo 不一定是第一个
            return "发现锁： " + setCopy.stream().findFirst().get();
        } else {
            List<String> addLock = new ArrayList<>();
            addLock.add("add_" + id + "_" + fileId);
            addLock.add("add_" + "5_" + fileId);
            addLock.add("add_" + "7_" + fileId);
            try {
                //2.查询所有父节点id  从ES差  某个节点的信息 祖先节点List  去除0
                set.addAll(addLock);
                set.expire(60, TimeUnit.SECONDS);
                set.forEach(System.out::println);
                System.out.println("文件正在写入");
                Thread.sleep(20000);

                set.forEach(System.out::println);
            } catch (Exception e) {
                Boolean unLock = set.removeAll(addLock);
                e.printStackTrace();
            } finally {
                System.out.println("释放锁");
                Boolean unLock = set.removeAll(addLock);
            }
            return "用户： " + userID + "文件上传、文件夹新建 " + "结果：  成功";
        }*/
        return "lua";
    }

    @GetMapping("lua")
    @ApiOperation(value = "1.lua", notes = "测试redis写入")
    @ResponseBody
    public String lua(@RequestParam(defaultValue = "001") String userID, String id, String fileId) {

        List<String> lockcheck = new ArrayList<>();
        lockcheck.add("delete_" + id);  //orgId_1_delete
        lockcheck.add("delete_" + "5"); //orgId_5_delete
        lockcheck.add("delete_" + "7"); //orgId_7_delete
        lockcheck.add("move_" + id);  // ordId_X_move_1
        lockcheck.add("move_" + "5"); // ordId_X_move_5
        lockcheck.add("move_" + "7"); // ordId_X_move_7
        lockcheck.add(id + "_move");  //移除  // ordId_1_move_X
        lockcheck.add("5" + "_move"); // ordId_5_move_X
        lockcheck.add("7" + "_move"); // ordId_7_move_X


        String r = redissonClient.getScript().eval(RScript.Mode.READ_ONLY,
                "return redis.call('get', '002_7_add')", RScript.ReturnType.VALUE);


        List<String> rs = redissonClient.getScript().eval(RScript.Mode.READ_ONLY,
                "return redis.call('keys', '002')", RScript.ReturnType.STATUS);

        Boolean c = redissonClient.getScript().eval(RScript.Mode.READ_ONLY,
                "return redis.call('keys', '*') 0", RScript.ReturnType.STATUS);

        System.out.println(r);
        RScript rScript = redissonClient.getScript();


        return "ok";
    }

    /**
     * 5 节点删除
     * <p>
     * 1.1 判断 5节点上是否有新增锁
     * 1.2 判断 5节点上是否有移动锁
     * 1.2
     */
    @GetMapping("delete")
    @ApiOperation(value = "2.文件删除 ", notes = "节点删除")
    @ResponseBody
    public String delete(@RequestParam(defaultValue = "001") String userID, @RequestParam(defaultValue = "5") String id) {

        //1.查询所有父节点id  从ES获取  某个节点的信息 祖先节点List  去除0
        List<String> deleteLockCheck = new ArrayList<>();
        deleteLockCheck.add(id + "_move");
        deleteLockCheck.add("7" + "_move");
        deleteLockCheck.add("move_" + id);

        List<String> sourceAddLockCheck = new ArrayList<>();
        sourceAddLockCheck.add("add_" + id);
        RSet<String> set = redissonClient.getSet(userID);
        Set<String> setCopy=new HashSet<>();
        setCopy.addAll(set);

        Boolean add=false;
        for (String addLock:sourceAddLockCheck) {
            add = setCopy.stream().anyMatch(o -> o.startsWith(addLock));
            System.out.println(add);
            if(add){
                System.out.println("发现并发 写锁");
                return "发现锁 1：" + addLock;
            }
        }
        Boolean s = setCopy.retainAll(deleteLockCheck);
        if (setCopy.size() > 0) {
            System.out.println("发现并发锁");
            return "发现锁 2：" + setCopy.stream().findFirst().get();
        } else {
            // 加删除锁
            // 获取被删除节点的 祖先路径  5 、7

            List<String> deleteLock = new ArrayList<>();
            deleteLock.add("delete_" + id);

            // 只有被删除节点锁  祖先可以 复制、移动、 新增、 删除

            try {
                set.addAll(deleteLock);
                set.expire(300, TimeUnit.SECONDS);
                System.out.println("节点正在删除中....");
                Thread.sleep(15000);
                System.out.println("节点删除结束");
            } catch (Exception e) {
                e.printStackTrace();
                set.removeAll(deleteLock);
            } finally {
                set.removeAll(deleteLock);
            }

        }
        return "用户： " + userID + "删除成功";
    }

    /**
     * 6节点移动到5节点
     * <p>
     * 1.1 判断 6节点上是否有新增锁
     * 1.2 判断 6 节点上是否有移动锁  7 是否移动
     * 1.3 判断 6 节点上有没有删除锁  7节点是否删除
     * <p>
     * <p>
     * 2.1 判断5 节点是否删除  7 节点是否删除
     * 2.2 判断 5 节点是被否移动 7 节点是否被移动
     */
    @GetMapping("move")
    @ApiOperation(value = "3.文件移动 ", notes = "节点删除")
    @ResponseBody
    public String move(@RequestParam(defaultValue = "001") String userID, @RequestParam(defaultValue = "6") String id_source, @RequestParam(defaultValue = "5") String id_dest) {

        //1.先处理 源 6 节点 目标查询所有父节点id  从ES获取


        List<String> sourceLockCheck = new ArrayList<>();
        List<String> sourceAddLockCheck = new ArrayList<>();
        sourceAddLockCheck.add("add_" + id_source);
        sourceLockCheck.add(id_source + "_move");
        sourceLockCheck.add("7" + "_move");
        sourceLockCheck.add("delete_" + id_source);
        sourceLockCheck.add("delete_" + "7");


        RSet<String> set = redissonClient.getSet(userID);
        Set<String> set2 = new HashSet<>(set.size());
        Set<String> setCopy=new HashSet<>(set.size());
        setCopy.addAll(set);
        set2.addAll(set);
        Boolean s = setCopy.retainAll(sourceLockCheck);
        boolean add=false;
        for (String addLock:sourceAddLockCheck) {
             add = setCopy.stream().anyMatch(o -> o.startsWith(addLock));
              if(add){
                  System.out.println("发现并发 写锁");
                  return "发现锁：" + addLock;
              }
        }
        if (setCopy.size() > 0) {
            System.out.println("发现并发锁");
            return "发现锁：" + setCopy.stream().findFirst().get();
        } else {
            // 加删除锁
            // 获取被删除节点的 祖先路径  5 、7
            //源文件锁
            List<String> source_lock = new ArrayList<>();
            //目标锁
            List<String> dest_lock = new ArrayList<>();
            source_lock.add(id_source + "_move");
            source_lock.add("add_" + "7_"+ id_source);
            try {
                set.addAll(source_lock);
                set.expire(300, TimeUnit.SECONDS);
                // 源节点加锁完成
                System.out.println("源节点加锁完成");
                // 处理目标节点 5
                System.out.println(" 处理目标节点" + id_dest);

                List<String> lockcheck = new ArrayList<>();
                lockcheck.add("delete_" + id_dest);
                lockcheck.add("delete_" + "7");
                lockcheck.add("move_" + id_dest);
                lockcheck.add("move_" + "7");
                Boolean dest = set2.retainAll(lockcheck);
                if (set2.size() > 0) {
                    System.out.println("发现并发锁");
                    throw new Exception("目标加锁失败");
                } else {

                    dest_lock.add("move_" + id_dest);
                    dest_lock.add("add_" + "7_"+id_dest);
                    set.addAll(dest_lock);
                    System.out.println("目标加锁成功 ..开始移动逻辑");
                    Thread.sleep(20000);
                    System.out.println("目标加锁成功 移动逻辑结束");
                }

            } catch (Exception e) {
                e.printStackTrace();
                set.removeAll(source_lock);
                set.removeAll(dest_lock);
            } finally {
                set.removeAll(source_lock);
                set.removeAll(dest_lock);
            }

        }
        return "用户： " + userID + "文件夹移动成功";
    }

    /**
     * 6节点复制到5节点
     * <p>
     * 1.1 判断 6节点上是否有新增锁
     * 1.2 判断 6 节点上是否有移动锁  7 是否移动
     * 1.3 判断 6 节点上有没有删除锁  7节点是否删除
     * <p>
     * <p>
     * 2.1 判断5 节点是否删除  7 节点是否删除
     * 2.2 判断 5 节点是被否移动 7 节点是否被移动
     */
    @GetMapping("copy")
    @ApiOperation(value = "4.文件复制 ", notes = "文件复制")
    @ResponseBody
    public String copy(@RequestParam(defaultValue = "001") String userID, @RequestParam(defaultValue = "6") String id_source, @RequestParam(defaultValue = "5") String id_dest) {

        //1.先处理 源 6 节点 目标查询所有父节点id  从ES获取


        List<String> parents = new ArrayList<>();
        List<String> sourceAddLockCheck = new ArrayList<>();
        sourceAddLockCheck.add("add_" + id_source);
        parents.add(id_source + "_move");
        parents.add("7" + "_move");
        parents.add("delete_" + id_source);
        parents.add("delete_" + "7");


        RSet<String> set = redissonClient.getSet(userID);
        Set<String> setCopy=new HashSet<>(set.size());
        setCopy.addAll(set);
        Set<String> set2 = new HashSet<>(set.size());
        set2.addAll(set);
        Boolean s = setCopy.retainAll(parents);
        boolean add=false;
        for (String addLock:sourceAddLockCheck) {
            add = setCopy.stream().anyMatch(o -> o.startsWith(addLock));
            if(add){
                System.out.println("发现并发 写锁");
                return "发现锁：" + addLock;
            }
        }
        if (setCopy.size() > 0) {
            System.out.println("发现并发锁");
            return "发现锁：" + setCopy.stream().findFirst().get();
        } else {
            // 加删除锁
            // 获取被删除节点的 祖先路径  5 、7
            List<String> lock = new ArrayList<>();
            List<String> parents2 = new ArrayList<>();
            parents2.add(id_source + "_move");
            parents2.add("add_" + "7_"+id_source);
            try {
                set.addAll(parents2);
                set.expire(300, TimeUnit.SECONDS);
                // 源节点加锁完成
                System.out.println("源节点加锁完成");
                // 处理目标节点 5
                System.out.println(" 处理目标节点" + id_dest);

                List<String> lockcheck = new ArrayList<>();
                lockcheck.add("delete_" + id_dest);
                lockcheck.add("delete_" + "7");
                lockcheck.add("move_" + id_dest);
                lockcheck.add("move_" + "7");
                Boolean dest = set2.retainAll(lockcheck);
                if (set2.size() > 0) {
                    System.out.println("发现并发锁");
                    throw new Exception("目标加锁失败");
                } else {

                    lock.add("move_" + id_dest);
                    lock.add("add_" + "7_"+id_dest);
                    set.addAll(lock);
                    System.out.println("目标加锁成功 ..开始移动逻辑");
                    Thread.sleep(30000);
                }

            } catch (Exception e) {
                e.printStackTrace();
                set.removeAll(parents2);
                set.removeAll(lock);
            } finally {
                set.removeAll(parents2);
                set.removeAll(lock);
            }

        }
        return "用户： " + userID + "文件夹移动成功";
    }


}
