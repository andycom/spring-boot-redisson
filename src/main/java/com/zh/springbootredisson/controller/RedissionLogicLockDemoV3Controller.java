package com.zh.springbootredisson.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.redisson.api.RScript;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

/**
 * 7
 * / \
 * 5   6
 * /\  /\
 * 1 2 3 4
 * <p>
 * <p>
 *
 * 使用 key value 实现锁
 * 锁的含义体现在key 上
 * 多元操作
 *
 * 锁的信息包含  四种操作  1、add   2、delete   3、move  4、copy
 *
 * 001_1_delete 组织ID 为001 的用户删除 1 节点
 * 002_5_move_6 组织ID 为001 的用户 5节点移动到6节点
 * 技术实现：Redission \ redis  \  lua 脚本
 * 1.lua 封装获取锁的逻辑 保证原子性
 * 2.Redis String  key 保存org 持有的锁  设置超时时间
 * 3.锁的续期参考Redission
 * 4.java 逻辑组装要校验的二元操作信息和需要写入Redis中的二元操作信息
 *
 *
 * 缺点： 被锁的节点信息用户看不到  lua 封装返回true or false
 *
 *
 */
@RestController
@Api(value = "0.lua")
@RequestMapping("v3")
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
    public String add(@RequestParam(defaultValue = "001") String userID, String id, String fileId) throws InterruptedException {


        //0  Java逻辑组织组要校验的锁

        List<String> lockcheck = new ArrayList<>();
        lockcheck.add(userID+"_" + id+"_delete");  //orgId_1_delete
        lockcheck.add(userID+"_" + "5"+"_delete"); //orgId_5_delete
        lockcheck.add(userID+"_" + "7"+"_delete"); //orgId_7_delete
        lockcheck.add(userID+"_move_" + id);  //  移入 ordId_1908730912_move_1
        lockcheck.add(userID+"_move_" + "5"); // ordId_23i2uy3i1_move_5
        lockcheck.add(userID+"_move_" + "7"); // ordId_move_7_8392384      8392384移入7
        lockcheck.add(userID+"_"+id + "_move");  //移出  // ordId_1_move_090329423
        lockcheck.add(userID+"_5" + "_move"); // ordId_5_move_9345783429
        lockcheck.add(userID+"_7" + "_move"); // ordId_7_move_92374237


        // 1.初始化redis中的数据 lua 脚本入参
        RSet<String> set = redissonClient.getSet(userID+"ok"+fileId);
        set.addAll(lockcheck);
        set.expire(135,TimeUnit.SECONDS);

        List<String> addLock = new ArrayList<>();
        addLock.add(userID+"_add_" + id + "_" + fileId);
        addLock.add(userID+"_add_" + "5_" + fileId);
        addLock.add(userID+"_add_" + "7_" + fileId);
        RSet<String> setLock = redissonClient.getSet(userID+"ok"+fileId+"Lock");
        setLock.addAll(addLock);
        setLock.expire(135,TimeUnit.SECONDS);
        // 脚本执行准备阶段
        RScript script = redissonClient.getScript(StringCodec.INSTANCE);
        String luaAdd= "return false";  //lua 读取失败默认返回FALSE

        try {
            File file = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX +"lua/add.lua");
            luaAdd = FileUtils.readFileToString(file,"UTF-8");
            System.out.println("lua file: " + luaAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 组装lua 入参
        List<Object> keys = new ArrayList<>();
        keys.add("001_*");
        keys.add(userID+"ok"+fileId);
        keys.add(userID+"ok"+fileId+"Lock");
        Object[] args = new Object[1];
        args[0] = "001_*";
        List<Object> entity = script.eval(RScript.Mode.READ_ONLY, "return redis.call('keys', KEYS[1])",  RScript.ReturnType.MULTI, keys);
        List<Object> entity2 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[2])",  RScript.ReturnType.MULTI, keys);
        List<Object> entity3 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[3])",  RScript.ReturnType.MULTI, keys);
        // lua 脚本原子操作获取锁 获取add 锁（原子操作）   文件夹新增、文件上传
        Boolean lock=script.eval(RScript.Mode.READ_ONLY, luaAdd,  RScript.ReturnType.BOOLEAN, keys);

       if(lock){
           System.out.println("获取到逻辑锁 开始执行文件新增");
           // 模拟上传文件操作
           Thread.sleep(20000);
           // todo 锁续期
           System.out.println("获取到逻辑锁 文件新增结束");
           // todo 锁删除
           for(String s:addLock){
             redissonClient.getBucket(s).delete();
           }

       }else{
           System.out.println("未获取逻辑锁 安排重试或者任务失败");
           // todo 删除准备信息
       }

        System.out.println("新增文件业务结束");






        return "lua";
    }


}
