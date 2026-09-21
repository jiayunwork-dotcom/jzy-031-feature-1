package engine

// slidingWindowMultiLua is the N-key atomic weighted sliding window. Each key
// may have its own window length; the effective count of every key is computed
// and the first over-limit key denies, otherwise all current-window counters
// are incremented atomically.
//
// ARGV layout: n, [limit, window_seconds] x n, peek
const slidingWindowMultiLua = `
local n = tonumber(ARGV[1])
local peek = tonumber(ARGV[#ARGV])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local cas = {}
local cbs = {}
local wms = {}
local effs = {}
local firstDeny = -1
for i = 1, n do
  local limit  = tonumber(ARGV[2 + (i-1)*2])
  local win_ms = tonumber(ARGV[3 + (i-1)*2]) * 1000
  local cur = math.floor(now / win_ms) * win_ms
  local elapsed = now - cur
  local data = redis.call('HMGET', KEYS[i], 'a', 'ca', 'cb')
  local a = tonumber(data[1]); local ca = tonumber(data[2]); local cb = tonumber(data[3])
  if a == nil then a=cur; ca=0; cb=0
  elseif a == cur-win_ms then cb=ca; ca=0
  elseif a ~= cur then cb=0; ca=0 end
  a = cur
  local weight = math.max(0, (win_ms - elapsed) / win_ms)
  local eff = cb * weight + ca
  cas[i]=ca; cbs[i]=cb; wms[i]=win_ms; effs[i]=eff
  if eff >= limit and firstDeny == -1 then firstDeny = i end
end

local out = {}
if firstDeny ~= -1 then
  for i = 1, n do
    local limit = tonumber(ARGV[2 + (i-1)*2])
    out[#out+1] = (i < firstDeny) and 1 or 0
    out[#out+1] = math.max(0, limit - math.ceil(effs[i]))
    out[#out+1] = wms[i] - (now - (math.floor(now/wms[i])*wms[i]))
  end
  out[#out+1] = firstDeny
  return out
end

for i = 1, n do
  local limit = tonumber(ARGV[2 + (i-1)*2])
  local win_ms = wms[i]
  local cur = math.floor(now / win_ms) * win_ms
  local ca = cas[i] + 1
  redis.call('HSET', KEYS[i], 'a', cur, 'ca', ca, 'cb', cbs[i])
  redis.call('PEXPIRE', KEYS[i], 2 * win_ms + 1000)
  out[#out+1] = 1
  out[#out+1] = math.max(0, limit - math.ceil(cbs[i] * math.max(0,(win_ms-(now-cur))/win_ms) + ca))
  out[#out+1] = win_ms - (now - cur)
end
out[#out+1] = 0
return out
`
