local function scan(key)
    local cursor = 0
    local keynum = 0
    local r={}
    repeat
        local res = redis.call("scan", cursor, "match", key,'COUNT',100)

        if (res ~= nil and #res >= 0) then
            redis.replicate_commands()
            cursor = tonumber(res[1])
            local ks = res[2]
            keynum = #ks
            for i=1,keynum,1 do
                local k = tostring(ks[i])
                table.insert(r, k)
                print("\"命中的逻辑锁\"",k)
            end
        end
    until (cursor <= 0)

    return r
end

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
---- local userLocks = redis.call('keys', KEYS[1]); local luaUserLocks = {}; for i = 1, #userLocks do luaUserLocks[i] = userLocks[i]  end;
local userLocks=scan(KEYS[1]);
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
