local function locksCheck(checkKeys, lockKeys)
    local re=false
    for i = 1, #checkKeys
    do for j=1 ,#lockKeys
       do  local r0=string.match(lockKeys[j], checkKeys[i]);
            if(not r0)
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
---- keys[2] 源需要检查的锁
local sourceLocksCheck=redis.call('smembers',KEYS[2]); local luaSourceCheckKeys = {}; for i = 1, #sourceLocksCheck do luaSourceCheckKeys[i] = sourceLocksCheck[i]  end;
---- keys[3] 目标需要检查的锁
local destLocksCheck=redis.call('smembers',KEYS[3]); local luaDestCheckKeys = {}; for i = 1, #destLocksCheck do luaDestCheckKeys[i] = destLocksCheck[i]  end;
---- keys[4] 目标锁
local supUserKeys=redis.call('smembers',KEYS[4]); local luaSupUserKeys = {}; for i = 1, #supUserKeys do luaSupUserKeys[i] = supUserKeys[i]  end;

if(locksCheck(sourceLocksCheck, luaUserLocks) or locksCheck(checkKeys_2, luaUserLocks))
then
    return false
else
    for k = 1, #luaSupUserKeys
      do  redis.call('set',luaSupUserKeys[k],'1')
          redis.call('expire', luaSupUserKeys[k], 90)
    end
    return true
end
