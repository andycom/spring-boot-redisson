---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by admin.
--- DateTime: 2022/1/18 16:39
---


----redis中的锁  subuserId
----local lockKeys = {'001_1_delete','001_5_delete','001_7_delete','001_1_move_9090','001_9091_move_1'};
local lockKeys = {'001_1_delete','001_15_delete','001_17_delete','001_11_move_9090','001_9091_move_1'};
---- 目标锁 入参目标锁 ARGS[]
local supUserKeys = {'001_1_add_9','001_5_add_9','001_7_add_9'};
---- 需要检查的锁  入参需要检查的锁  KEYS[]
local checkKeys= {'001_1_delete','001_5_delete','001_7_delete','001_1_move','001_5_move','001_7_move'};

---- lua 中add 锁的核心逻辑


local function join_and_filter(checkKeys, lockKeys)
    local f = false;
    local re = false;
    for i = 1, #checkKeys
    do print(checkKeys[i])
        for j=1 ,#lockKeys
        do local tstring=string.match(lockKeys[j], checkKeys[i]);
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

local result = join_and_filter(checkKeys, lockKeys);

print(result)
return result



local result = redis.call("SCAN", cursor, "MATCH", KEYS[1], "COUNT", 10);
