local function join_and_filter(checkKeys, lockKeys)
    local f=false;
    local re=false
    for i = 1, #checkKeys
    do for j=1 ,#lockKeys
       do  local tstring=string.match(lockKeys[j], checkKeys[i]);
            if(not tstring)
            then
                f = false
            else
                re = true
            break
            end;
        end;
    end;
        if(f)
        then
            return f
        end;
    return re
end

----keys[1] 用户持有的锁
local keys = redis.call('keys', KEYS[1]); local keyValuePairs = {}; for i = 1, #keys do keyValuePairs[i] = keys[i]  end;

---- keys[2]  需要检查的锁
local checkKeys_1=redis.call("smembers",KEYS[2]); local checkKeys = {}; for i = 1, #checkKeys_1 do checkKeys[i] = checkKeys_1[i]  end;
---- keys[3]  目标锁
local supUserKeys_1=redis.call("smembers",KEYS[3]); local supUserKeys = {}; for i = 1, #supUserKeys_1 do supUserKeys[i] = supUserKeys_1[i]  end ;

if(join_and_filter(checkKeys, keyValuePairs))
then
    return false
else
    for k = 1, #supUserKeys
      do  redis.call("set",supUserKeys[k],'1')
          redis.call('expire', supUserKeys[k], 30)
    end
    return true
end
