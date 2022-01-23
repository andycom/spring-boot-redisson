local function locksCheck(checkKeys, lockKeys)
    local re=false
    for i = 1, #checkKeys
    do for j=1 ,#lockKeys
       do  local t0=string.match(lockKeys[j], checkKeys[i]);
            if(not t0)
            then
            else
                re = true
            break
            end;
        end;
    end;
    return re
end

---- keys[1] 用户持有的锁
local userLocks = redis.call('keys', KEYS[1]); local luaUserLocks = {}; for i = 1, #userLocks do luaUserLocks[i] = userLocks[i]  end;
---- keys[2] 需要检查的锁
local userLocksCheck=redis.call('smembers',KEYS[2]); local luaSourceCheckKeys = {}; for i = 1, #userLocksCheck do luaSourceCheckKeys[i] = userLocksCheck[i]  end;
---- keys[3] 目标锁
local supUserKeys=redis.call('smembers',KEYS[3]); local luaSupUserKeys = {}; for i = 1, #supUserKeys do luaSupUserKeys[i] = supUserKeys[i]  end;

if(locksCheck(luaSourceCheckKeys, userLocks))
then
    return false
else
    for k = 1, #luaSupUserKeys
      do  redis.call('set',luaSupUserKeys[k],'1')
          redis.call('expire', luaSupUserKeys[k], 30)
    end
    return true
end
