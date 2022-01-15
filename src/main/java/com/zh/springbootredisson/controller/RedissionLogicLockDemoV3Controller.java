package com.zh.springbootredisson.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.redisson.api.RScript;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

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
@Api(value = "0.lua")
@RequestMapping("v2")
public class RedissionLogicLockDemoV3Controller {

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
    @ApiOperation(value = "1.增加文件 、文件夹", notes = "")
    @ResponseBody
    public String add(@RequestParam(defaultValue = "001") String userID, String id, String fileId) {


        //0  查看祖先路径有没有删除锁  移动锁 出入

        List<String> lockcheck = new ArrayList<>();
        lockcheck.add(userID+"_" + id+"_delete");  //orgId_1_delete
        lockcheck.add(userID+"_" + "5"+"_delete"); //orgId_5_delete
        lockcheck.add(userID+"_" + "7"+"_delete"); //orgId_7_delete
        lockcheck.add(userID+"_move_" + id);  //  移入 ordId_1908730912_move_1
        lockcheck.add(userID+"_move_" + "5"); // ordId_23i2uy3i1_move_5
        lockcheck.add(userID+"_move_" + "7"); // ordId_8392384_move_7
        lockcheck.add(userID+"_"+id + "_move");  //移出  // ordId_1_move_090329423
        lockcheck.add(userID+"_5" + "_move"); // ordId_5_move_9345783429
        lockcheck.add(userID+"_7" + "_move"); // ordId_7_move_92374237


        // TODO  list redisson 先预存到redis中 key 的逻辑设计
        // 使用redis set 获取入参 table


        RScript script = redissonClient.getScript(StringCodec.INSTANCE);

        // List<String> a = rScript.eval(RScript.Mode.READ_ONLY,"return redis.call('keys','*')",RScript.ReturnType.VALUE);
        List<Object> keys = new ArrayList<>();
        keys.add("001_*");
        keys.addAll(lockcheck);
        Object[] args = new Object[1];
        args[0] = "001_*";
        List<Object> entity = script.eval(RScript.Mode.READ_ONLY, "return redis.call('keys', KEYS[1])",  RScript.ReturnType.MULTI, keys);

        Boolean lock=script.eval(RScript.Mode.READ_ONLY, "return redis.call('keys', KEYS[1])",  RScript.ReturnType.BOOLEAN, keys);

        System.out.println(entity.size());



        RScript rScript = redissonClient.getScript();
        redissonClient.getBucket("foo").set("bar");
        String r = redissonClient.getScript().eval(RScript.Mode.READ_ONLY,
                "return redis.call('get', 'foo')", RScript.ReturnType.VALUE);

        System.out.println(r);
        redissonClient.getBucket("002_7_add").set("00001");
        String r2 = redissonClient.getScript().eval(RScript.Mode.READ_ONLY,
                "return redis.call('get', '002_7_add')", RScript.ReturnType.VALUE);
        System.out.println(r2);
        List<Object> rs = script.eval(RScript.Mode.READ_ONLY,
                "local keys = redis.call('keys', '*'); local keyValuePairs = {}; for i = 1, #keys do keyValuePairs[i] = keys[i]  end; return keyValuePairs", RScript.ReturnType.MULTI,Collections.emptyList());
        System.out.println(rs);
        Boolean c = script.eval(RScript.Mode.READ_ONLY,
                "local a= true;  return a", RScript.ReturnType.BOOLEAN);

        System.out.println(r);


        List<Object> res = script.eval(RScript.Mode.READ_ONLY,"return {1,2,3.3333,'foo',nil,'bar'}", RScript.ReturnType.MULTI, Collections.emptyList());

        String value = "test";
        script.eval(RScript.Mode.READ_WRITE, "redis.call('set', KEYS[1], ARGV[1])", RScript.ReturnType.VALUE, Arrays.asList("1"), value);

        String val = script.eval(RScript.Mode.READ_WRITE, "return redis.call('get', KEYS[1])", RScript.ReturnType.VALUE, Arrays.asList("foo"));

       // List<Object> entity = script.eval(RScript.Mode.READ_ONLY, "return redis.call('keys', KEYS[1])",  RScript.ReturnType.MULTI, Arrays.asList("001_*"));

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


}
