package engine

// slidingLogMultiLua is the N-key atomic sliding log. Each key is a ZSET;
// expired members are evicted on every key, the first key at capacity denies,
// otherwise one timestamped nonce is added to every key.
//
// ARGV layout: n, [limit, window_seconds, nonce] x n, peek
const slidingLogMultiLua = `
local n = tonumber(ARGV[1])
local peek = tonumber(ARGV[#ARGV])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local counts = {}
local wms = {}
local firstDeny = -1
for i = 1, n do
  local base = 2 + (i-1)*3
  local limit  = tonumber(ARGV[base])
  local win_ms = tonumber(ARGV[base+1]) * 1000
  local cutoff = now - win_ms
  redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', cutoff)
  local c = redis.call('ZCARD', KEYS[i])
  counts[i] = c; wms[i] = win_ms
  if c >= limit and firstDeny == -1 then firstDeny = i end
end

local out = {}
if firstDeny ~= -1 then
  for i = 1, n do
    local base = 2 + (i-1)*3
    local limit = tonumber(ARGV[base])
    local win_ms = wms[i]
    local oldest = redis.call('ZRANGE', KEYS[i], 0, 0, 'WITHSCORES')
    local reset = 0
    if oldest[2] ~= nil then
      reset = (tonumber(oldest[2]) + win_ms) - now
      if reset < 0 then reset = 0 end
    end
    out[#out+1] = (i < firstDeny) and 1 or 0
    out[#out+1] = math.max(0, limit - counts[i])
    out[#out+1] = reset
  end
  out[#out+1] = firstDeny
  return out
end

for i = 1, n do
  local base = 2 + (i-1)*3
  local limit  = tonumber(ARGV[base])
  local win_ms = wms[i]
  local nonce  = ARGV[base+2]
  redis.call('ZADD', KEYS[i], now, nonce)
  redis.call('PEXPIRE', KEYS[i], win_ms + 1000)
  out[#out+1] = 1
  out[#out+1] = math.max(0, limit - counts[i] - 1)
  out[#out+1] = win_ms
end
out[#out+1] = 0
return out
`
