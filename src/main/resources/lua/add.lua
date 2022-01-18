local function join_and_filter(checkKeys, lockKeys)
    local f=false;
    for i = 1, #checkKeys
    do for j=1 ,#lockKeys
       do  local tstring=string.match(lockKeys[j], checkKeys[i]);
            if(not tstring)
            then
                f=false
            else
                f = true
            break
            end;
        end;
    end;
        if(f)
        then
            return f
        end;
    return f
end

----keys[1] 用户持有的锁
local keys = redis.call('keys', KEYS[1]); local keyValuePairs = {}; for i = 1, #keys do keyValuePairs[i] = keys[i]  end;

---- keys[2]  需要检查的锁
local checkKeys=redis.call("smembers",KEYS[2]);
---- keys[3]  目标锁
local supUserKeys=redis.call("smembers",KEYS[3]);

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
