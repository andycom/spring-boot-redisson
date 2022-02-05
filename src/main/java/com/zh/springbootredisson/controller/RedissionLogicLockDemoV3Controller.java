package com.zh.springbootredisson.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.io.FileUtils;
import org.redisson.api.RScript;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 7
 * / \
 * 5   6
 * /\  /\
 * 1 2 3 4
 * <p>
 * <p>
 * <p>
 * 使用 key value 实现锁
 * 锁的含义体现在key 上
 * 多元操作
 * <p>
 * 锁的信息包含  四种操作  1、add   2、delete   3、move  4、copy
 * <p>
 * 001_1_delete 组织ID 为001 的用户删除 1 节点
 * 002_5_move_6 组织ID 为001 的用户 5节点移动到6节点
 * 技术实现：Redission \ redis  \  lua 脚本
 * 1.lua 封装获取锁的逻辑 保证原子性
 * 2.Redis String  key 保存org 持有的锁  设置超时时间
 * 3.锁的续期参考Redission
 * 4.java 逻辑组装要校验的二元操作信息和需要写入Redis中的二元操作信息
 * <p>
 * <p>
 * 缺点： 被锁的节点信息用户看不到  lua 封装返回true or false
 */
@RestController
@Api(value = "0.lua")
@RequestMapping("v3")
public class RedissionLogicLockDemoV3Controller {

    @Autowired
    private RedissonClient redissonClient;


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
        lockcheck.add(userID + "_" + id + "_delete");  //orgId_1_delete_fileID  //一元操作增加fileID 保证锁唯一
        lockcheck.add(userID + "_" + "5" + "_delete"); //orgId_5_delete
        lockcheck.add(userID + "_" + "7" + "_delete"); //orgId_7_delete
        lockcheck.add(userID + "_move_" + id);  //  移入 ordId_1908730912_move_1
        lockcheck.add(userID + "_move_" + "5"); //
        lockcheck.add(userID + "_move_" + "7"); // ordId_move_7_8392384      8392384移入7
        lockcheck.add(userID + "_" + id + "_move");  //移出  // ordId_1_move_090329423   二元操作本身保证唯一
        lockcheck.add(userID + "_5" + "_move"); // ordId_5_move_9345783429
        lockcheck.add(userID + "_7" + "_move"); // ordId_7_move_92374237


        // 1.初始化redis中的数据 lua 脚本入参
        RSet<String> set = redissonClient.getSet(userID + "ok" + fileId, StringCodec.INSTANCE);
        set.addAll(lockcheck);
        set.expire(135, TimeUnit.SECONDS);

        List<String> addLock = new ArrayList<>();
        addLock.add(userID + "_" + id + "_add_" + fileId);  // 1节点被加上一个fileID
        addLock.add(userID + "_5_add_" + fileId);
        addLock.add(userID + "_7_add_" + fileId);
        RSet<String> setLock = redissonClient.getSet(userID + "ok" + fileId + "Lock", StringCodec.INSTANCE);
        setLock.addAll(addLock);
        setLock.expire(135, TimeUnit.SECONDS);
        // 脚本执行准备阶段
        RScript script = redissonClient.getScript(StringCodec.INSTANCE);
        String luaAdd = "return false";  //lua 读取失败默认返回FALSE
        String luaScan = "return false";  //lua 读取失败默认返回FALSE

        try {
            File file = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX + "lua/add.lua");
            luaAdd = FileUtils.readFileToString(file, "UTF-8");
            System.out.println("lua file: " + luaAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            File file = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX + "lua/scan.lua");
            luaScan = FileUtils.readFileToString(file, "UTF-8");
            System.out.println("lua file: " + luaScan);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 组装lua 入参
        List<Object> keys = new ArrayList<>();
        keys.add("001_*");
        keys.add(userID + "ok" + fileId);
        keys.add(userID + "ok" + fileId + "Lock");
        Object[] args = new Object[1];
        args[0] = "001_*";
        List<Object> entity = script.eval(RScript.Mode.READ_ONLY, luaScan, RScript.ReturnType.MULTI, keys);
        List<Object> entity2 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[2])", RScript.ReturnType.MULTI, keys);
        List<Object> entity3 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[3])", RScript.ReturnType.MULTI, keys);
        // lua 脚本原子操作获取锁 获取add 锁（原子操作）   文件夹新增、文件上传
        Boolean lock = script.eval(RScript.Mode.READ_ONLY, luaAdd, RScript.ReturnType.BOOLEAN, keys);

        if (lock) {
            System.out.println("获取到逻辑锁 开始执行文件新增");
            // 模拟上传文件操作
            Thread.sleep(10000);
            // todo 锁续期
            System.out.println("获取到逻辑锁 文件新增结束");
            // todo 锁删除
            for (String s : addLock) {
                redissonClient.getBucket(s).delete();
            }

        } else {
            System.out.println("未获取逻辑锁 安排重试或者任务失败");
            // todo 删除准备信息
        }

        System.out.println("新增文件业务结束");

        return "lua";
    }

    /**
     * @param userID 用户ID
     * @param id     被删除文件的parentID
     * @param fileId 被删除文件ID
     * @return
     * @throws InterruptedException 场景逻辑： 删除2 节点
     *                              1. 2 没有新增 userID_2_add_fileID  2的子孙节点有没有 add 锁 、有没有复制入 、没有移动入
     *                              2. 2 有没有移入 userId_move_2_fileId
     *                              3. 2 有没有复制入 userID_copy_2_fileId
     *                              2-5-7 路径需要判断
     *                              4. 2 的祖先路径 有没有被移走
     *                              userId_7_move_20220323
     *                              userId_5_move_93938023
     *                              5. 2 的祖先有没有被复制
     *                              userId_7_copy_20220323
     *                              userId_5_copy_93938023
     */
    @GetMapping("delete")
    @ApiOperation(value = "2.删除文件 、文件夹", notes = "网盘删除文件 文件夹")
    @ResponseBody
    public String delete(@RequestParam(defaultValue = "001") String userID, @RequestParam(defaultValue = "2") String id, String fileId) throws InterruptedException {


        //0  Java逻辑组织组要校验的锁

        List<String> lockcheck = new ArrayList<>();
        lockcheck.add(userID + "_" + id + "_add");  //orgId_2_add_fileID  // 2 节点文件夹有没有新增  （控制被删除文件夹下所有子孙节点是否新增）
        lockcheck.add(userID + "_move_" + id); //
        lockcheck.add(userID + "_copy_" + id); //
        lockcheck.add(userID + "_7_move");  //
        lockcheck.add(userID + "_5_move_"); //
        lockcheck.add(userID + "_5_copy"); //
        lockcheck.add(userID + "_7_copy");  //2 的祖先有没有被复制


        // 1.初始化redis中的数据 lua 脚本入参
        RSet<String> set = redissonClient.getSet(userID + "ok" + fileId, StringCodec.INSTANCE);
        set.addAll(lockcheck);
        set.expire(135, TimeUnit.SECONDS);

        List<String> addLock = new ArrayList<>();
        addLock.add(userID + "_" + id + "_delete");
        addLock.add(userID + "_5_add_" + id + "_d");  //删除2 节点  父路径加add 锁  子节点删除的时候 父路径不能被移动出  复制出  可以add
        addLock.add(userID + "_7_add_" + id + "_d");
        RSet<String> setLock = redissonClient.getSet(userID + "ok" + fileId + "Lock", StringCodec.INSTANCE);
        setLock.addAll(addLock);
        setLock.expire(135, TimeUnit.SECONDS);
        // 脚本执行准备阶段
        RScript script = redissonClient.getScript(StringCodec.INSTANCE);
        String luaAdd = "return false";  //lua 读取失败默认返回FALSE

        try {
            File file = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX + "lua/add.lua");
            luaAdd = FileUtils.readFileToString(file, "UTF-8");
            System.out.println("lua file: " + luaAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 组装lua 入参
        List<Object> keys = new ArrayList<>();
        keys.add("001_*");
        keys.add(userID + "ok" + fileId); //uuid +lock 唯一标识
        keys.add(userID + "ok" + fileId + "Lock");
        Object[] args = new Object[1];
        args[0] = "001_*";
        List<Object> entity = script.eval(RScript.Mode.READ_ONLY, "return redis.call('keys', KEYS[1])", RScript.ReturnType.MULTI, keys);
        List<Object> entity2 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[2])", RScript.ReturnType.MULTI, keys);
        List<Object> entity3 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[2])", RScript.ReturnType.MULTI, keys);
        // lua 脚本原子操作获取锁 获取add 锁（原子操作）   文件夹新增、文件上传
        Boolean lock = script.eval(RScript.Mode.READ_ONLY, luaAdd, RScript.ReturnType.BOOLEAN, keys);

        if (lock) {
            System.out.println("获取到逻辑锁 开始执行文件删除");
            // 模拟上传文件操作
            Thread.sleep(20000);
            // todo 锁续期
            System.out.println("获取到逻辑锁 文件删除结束");
            // todo 锁删除
            for (String s : addLock) {
                redissonClient.getBucket(s).delete();
            }

        } else {
            System.out.println("其他子账户正在操作 请稍后再试");
            // todo 删除准备信息
        }

        System.out.println("文件删除 文件夹删除  业务结束");

        return "delete";
    }

    /**
     * 移动锁 移动锁不能复用脚本， 移动锁 是先锁 from 再锁to 的逻辑
     * <p>
     * 移动对于源来说  是删除，移动某个节点（9080） 这个节点加 移出锁 模板节点加 移入锁
     * <p>
     * 6节点移动到5节点
     * * <p>
     * * 1.1 判断 6节点上是否有新增锁
     * * 1.2 判断 6 节点上是否有移动锁  7 是否移动
     * * 1.3 判断 6 节点上有没有删除锁  7节点是否删除
     * * <p>
     * * <p>
     * * 2.1 判断5 节点是否删除  7 节点是否删除
     * * 2.2 判断 5 节点是被否移动 7 节点是否被移动
     *
     * @param userID
     * @param sourceParentDirId
     * @param sourceId
     * @param destId
     * @return
     * @throws InterruptedException
     */

    @GetMapping("move")
    @ApiOperation(value = "3.移动文件 文件夹", notes = "网盘移动文件 文件夹")
    @ResponseBody
    public String move(@RequestParam(defaultValue = "001") String userID, @RequestParam(defaultValue = "7") String sourceParentDirId, @RequestParam(defaultValue = "6") String sourceId, @RequestParam(defaultValue = "5") String destId) throws InterruptedException {


        String result = "";
        String TaskId = "taskId";


        //0.源 锁检查  6节点
        List<String> sourceLockCheck = new ArrayList<>();
        //检查 6 节点下是不是在新增
        sourceLockCheck.add(userID + "_" + sourceId + "_add");
        // 检查有没有文件在移入 6    001_move_6_09879   节点09876 正在移入 节点6
        sourceLockCheck.add(userID + "_move_" + sourceId);
        // 检查 6 节点有没有被移出  001_6_move_392  节点6 正在移出到 392
        sourceLockCheck.add(userID + "_" + sourceId + "_move");
        // 检查 7 节点有没有被移出  001_6_move_1396  节点7 正在移出到 1396
        sourceLockCheck.add(userID + "_" + "7" + "_move");
        // 检查 6 节点有没有被删除
        sourceLockCheck.add(userID + "_6" + "_delete");
        // 检查 7 节点有没有被删除
        sourceLockCheck.add(userID + "_7" + "_delete");
        // 6 节点有没有被复制  出  入
        // 检查有没有文件在复制 6 入   001_copy_6_9   节点9 正在复制入 节点9
        sourceLockCheck.add(userID + "_copy_" + sourceId);
        // 检查 6 节点有没有复制出  001_6_copy_192  节点6 正在复制到 192
        sourceLockCheck.add(userID + "_" + sourceId + "_copy");
        // 7 节点有没有被复制出
        // 检查 7 节点有没有辅助出  001_7_copy_92  节点7 正在复制到 92
        sourceLockCheck.add(userID + "_" + "7" + "_copy");


        //1.目标锁检查  5 节点检查

        List<String> destLockcheck = new ArrayList<>();
        // 5 节点是否在删除
        destLockcheck.add(userID + "_" + destId + "_delete");
        // 7 节点是否在删除
        destLockcheck.add(userID + "_" + "7" + "_delete");
        // 5 节点有没有被移动走
        destLockcheck.add(userID + "_" + destId + "_move_");
        // 7 节点有没有被移动走
        destLockcheck.add(userID + "_" + "7" + "_move_");
        // 5 节点有没有被复制
        destLockcheck.add(userID + "_" + destId + "_copy_");
        // 7 节点有没有被复制
        destLockcheck.add(userID + "_" + "7" + "_copy_");


        //2.目标锁  6 节点 和 5 节点目标锁 一次性set 目标锁

        //2.1 源文件锁

        List<String> addLock = new ArrayList<>();
        addLock.add(userID + "_" + sourceId + "_move_" + destId);  // 6 节点移动到5 节点下  6 节点锁信息

        addLock.add(userID + "_move_" + destId + "_" + sourceId);  //6 节点移动到5 节点下  5 节点锁信息

        addLock.add(userID + "_7_add_" + sourceId + "_d");  //  6 节点父级  加add  锁  方式被删除  被移动走


        //TODO 确认是不是同一父级节点 下移动
        //2.2 目标文件
        // 5 节点被移入
        addLock.add(userID + "_move_" + destId + "_" + sourceId);
        addLock.add(userID + "_7_add_" + destId);  // 父级节点新增逻辑

        // 3初始化Redis 中计算参数
        // 3.1 初始化sourceLockCheck 的Redis 数据
        RSet<String> sourceSet = redissonClient.getSet(userID + TaskId + "sourceLockCheck", StringCodec.INSTANCE);
        sourceSet.addAll(sourceLockCheck);
        sourceSet.expire(135, TimeUnit.SECONDS);

        // 3.2 初始化destLockcheck 的Redis 数据
        RSet<String> destSet = redissonClient.getSet(userID + TaskId + "destLockcheck", StringCodec.INSTANCE);
        destSet.addAll(destLockcheck);
        destSet.expire(135, TimeUnit.SECONDS);

        // 3.3 目标锁
        RSet<String> addLockSet = redissonClient.getSet(userID + TaskId + "addLock", StringCodec.INSTANCE);
        addLockSet.addAll(addLock);
        addLockSet.expire(135, TimeUnit.SECONDS);
        // 脚本执行准备阶段
        RScript script = redissonClient.getScript(StringCodec.INSTANCE);
        String luaAdd = "return false";  //lua 读取失败默认返回FALSE

        try {
            File file = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX + "lua/move.lua");
            luaAdd = FileUtils.readFileToString(file, "UTF-8");
            System.out.println("lua file: " + luaAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 组装lua 入参
        List<Object> keys = new ArrayList<>();
        keys.add("001_*");
        keys.add(userID + TaskId + "sourceLockCheck"); //uuid +lock 唯一标识
        keys.add(userID + TaskId + "destLockcheck");
        keys.add(userID + TaskId + "addLock");

        List<Object> entity = script.eval(RScript.Mode.READ_ONLY, "return redis.call('keys', KEYS[1])", RScript.ReturnType.MULTI, keys);
        List<Object> entity2 = script.eval(RScript.Mode.READ_ONLY, "local checkKeys_2=redis.call('smembers',KEYS[2]); local checkKeys_dest = {}; for i = 1, #checkKeys_2 do checkKeys_dest[i] = checkKeys_2[i]  end; return checkKeys_dest", RScript.ReturnType.MULTI, keys);
        List<Object> entity3 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[3]) ", RScript.ReturnType.MULTI, keys);
        List<Object> entity4 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[4]) ", RScript.ReturnType.MULTI, keys);

        // lua 脚本原子操作获取锁 获取add 锁（原子操作）   文件夹新增、文件上传
        Boolean lock = script.eval(RScript.Mode.READ_ONLY, luaAdd, RScript.ReturnType.BOOLEAN, keys);

        if (lock) {
            System.out.println("获取到逻辑锁 开始执行文件移动");
            // 模拟上传文件操作
            Thread.sleep(15000);
            // todo 锁续期
            System.out.println("获取到逻辑锁 文件移动结束");
            // todo 锁删除
            for (String s : addLock) {
                redissonClient.getBucket(s).delete();
            }
            StringBuilder stringBuilder = new StringBuilder(TaskId);
            stringBuilder.append("获取到逻辑锁 文件移动结束");
            result = stringBuilder.toString();

        } else {
            System.out.println("其他子账户正在操作 请稍后再试");
            StringBuilder stringBuilder = new StringBuilder(TaskId);
            stringBuilder.append("其他子账户正在操作 请稍后再试");
            result = stringBuilder.toString();
        }

        System.out.println("文件删除 文件夹删除  业务结束");

        return result;
    }

    /**
     * 移动锁 移动锁不能复用脚本， 移动锁 是先锁 from 再锁to 的逻辑
     * <p>
     * 移动对于源来说  是删除，移动某个节点（9080） 这个节点加 移出锁 模板节点加 移入锁
     * <p>
     * 6节点移动到5节点
     * * <p>
     * * 1.1 判断 6节点上是否有新增锁
     * * 1.2 判断 6 节点上是否有移动锁  7 是否移动
     * * 1.3 判断 6 节点上有没有删除锁  7节点是否删除
     * * <p>
     * * <p>
     * * 2.1 判断5 节点是否删除  7 节点是否删除
     * * 2.2 判断 5 节点是被否移动 7 节点是否被移动
     *
     * @param userID
     * @param sourceParentDirId
     * @param sourceId
     * @param destId
     * @return
     * @throws InterruptedException
     */

    @GetMapping("copy")
    @ApiOperation(value = "4.文件复制、文件夹复制", notes = "网盘复制文件 文件夹")
    @ResponseBody
    public String copy(@RequestParam(defaultValue = "001") String userID, @RequestParam(defaultValue = "7") String sourceParentDirId, @RequestParam(defaultValue = "6") String sourceId, @RequestParam(defaultValue = "5") String destId) throws InterruptedException {


        String result = "";
        String TaskId = "taskId";


        //0.源 锁检查  6节点
        List<String> sourceLockCheck = new ArrayList<>();
        //检查 6 节点下是不是在新增
        sourceLockCheck.add(userID + "_" + sourceId + "_add");
        // 检查有没有文件在移入 6    001_move_6_09879   节点09876 正在移入 节点6
        sourceLockCheck.add(userID + "_move_" + sourceId);
        // 检查 6 节点有没有被移出  001_6_move_392  节点6 正在移出到 392
        sourceLockCheck.add(userID + "_" + sourceId + "_move");
        // 检查 7 节点有没有被移出  001_6_move_1396  节点7 正在移出到 1396
        sourceLockCheck.add(userID + "_" + "7" + "_move");
        // 检查 6 节点有没有被删除
        sourceLockCheck.add(userID + "_6" + "_delete");
        // 检查 7 节点有没有被删除
        sourceLockCheck.add(userID + "_7" + "_delete");
        // 6 节点有没有被复制  出  入
        // 检查有没有文件在复制 6 入   001_copy_6_9   节点9 正在复制入 节点9
        sourceLockCheck.add(userID + "_copy_" + sourceId);
        // 检查 6 节点有没有复制出  001_6_copy_192  节点6 正在复制到 192
        sourceLockCheck.add(userID + "_" + sourceId + "_copy");
        // 7 节点有没有被复制出
        // 检查 7 节点有没有辅助出  001_7_copy_92  节点7 正在复制到 92
        sourceLockCheck.add(userID + "_" + "7" + "_copy");


        //1.目标锁检查  5 节点检查

        List<String> destLockcheck = new ArrayList<>();
        // 5 节点是否在删除
        destLockcheck.add(userID + "_" + destId + "_delete");
        // 7 节点是否在删除
        destLockcheck.add(userID + "_" + "7" + "_delete");
        // 5 节点有没有被移动走
        destLockcheck.add(userID + "_" + destId + "_move_");
        // 7 节点有没有被移动走
        destLockcheck.add(userID + "_" + "7" + "_move_");
        // 5 节点有没有被复制
        destLockcheck.add(userID + "_" + destId + "_copy_");
        // 7 节点有没有被复制
        destLockcheck.add(userID + "_" + "7" + "_copy_");


        //2.目标锁  6 节点 和 5 节点目标锁 一次性set 目标锁

        //2.1 源文件锁

        List<String> addLock = new ArrayList<>();
        addLock.add(userID + "_" + sourceId + "_move_" + destId);  // 6 节点移动到5 节点下  6 节点锁信息

        addLock.add(userID + "_move_" + destId + "_" + sourceId);  //6 节点移动到5 节点下  5 节点锁信息

        addLock.add(userID + "_7_add_" + sourceId + "_d");  //  6 节点父级  加add  锁  方式被删除  被移动走


        //TODO 确认是不是同一父级节点 下移动
        //2.2 目标文件
        // 5 节点被移入
        addLock.add(userID + "_move_" + destId + "_" + sourceId);
        addLock.add(userID + "_7_add_" + destId);  // 父级节点新增逻辑

        // 3初始化Redis 中计算参数
        // 3.1 初始化sourceLockCheck 的Redis 数据
        RSet<String> sourceSet = redissonClient.getSet(userID + TaskId + "sourceLockCheck", StringCodec.INSTANCE);
        sourceSet.addAll(sourceLockCheck);
        sourceSet.expire(135, TimeUnit.SECONDS);

        // 3.2 初始化destLockcheck 的Redis 数据
        RSet<String> destSet = redissonClient.getSet(userID + TaskId + "destLockcheck", StringCodec.INSTANCE);
        destSet.addAll(destLockcheck);
        destSet.expire(135, TimeUnit.SECONDS);

        // 3.3 目标锁
        RSet<String> addLockSet = redissonClient.getSet(userID + TaskId + "addLock", StringCodec.INSTANCE);
        addLockSet.addAll(addLock);
        addLockSet.expire(135, TimeUnit.SECONDS);
        // 脚本执行准备阶段
        RScript script = redissonClient.getScript(StringCodec.INSTANCE);
        String luaAdd = "return false";  //lua 读取失败默认返回FALSE

        try {
            File file = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX + "lua/move.lua");
            luaAdd = FileUtils.readFileToString(file, "UTF-8");
            System.out.println("lua file: " + luaAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 组装lua 入参
        List<Object> keys = new ArrayList<>();
        keys.add("001_*");
        keys.add(userID + TaskId + "sourceLockCheck"); //uuid +lock 唯一标识
        keys.add(userID + TaskId + "destLockcheck");
        keys.add(userID + TaskId + "addLock");

        List<Object> entity = script.eval(RScript.Mode.READ_ONLY, "return redis.call('keys', KEYS[1])", RScript.ReturnType.MULTI, keys);
        List<Object> entity2 = script.eval(RScript.Mode.READ_ONLY, "local checkKeys_2=redis.call('smembers',KEYS[2]); local checkKeys_dest = {}; for i = 1, #checkKeys_2 do checkKeys_dest[i] = checkKeys_2[i]  end; return checkKeys_dest", RScript.ReturnType.MULTI, keys);
        List<Object> entity3 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[3]) ", RScript.ReturnType.MULTI, keys);
        List<Object> entity4 = script.eval(RScript.Mode.READ_ONLY, "return redis.call('smembers',KEYS[4]) ", RScript.ReturnType.MULTI, keys);

        // lua 脚本原子操作获取锁 获取add 锁（原子操作）   文件夹新增、文件上传
        Boolean lock = script.eval(RScript.Mode.READ_ONLY, luaAdd, RScript.ReturnType.BOOLEAN, keys);

        if (lock) {
            System.out.println("获取到逻辑锁 开始执行文件移动");
            // 模拟上传文件操作
            Thread.sleep(15000);
            // todo 锁续期
            System.out.println("获取到逻辑锁 文件移动结束");
            // todo 锁删除
            for (String s : addLock) {
                redissonClient.getBucket(s).delete();
            }
            StringBuilder stringBuilder = new StringBuilder(TaskId);
            stringBuilder.append("获取到逻辑锁 文件移动结束");
            result = stringBuilder.toString();

        } else {
            System.out.println("其他子账户正在操作 请稍后再试");
            StringBuilder stringBuilder = new StringBuilder(TaskId);
            stringBuilder.append("其他子账户正在操作 请稍后再试");
            result = stringBuilder.toString();
        }

        System.out.println("文件删除 文件夹删除  业务结束");

        return result;
    }


}
