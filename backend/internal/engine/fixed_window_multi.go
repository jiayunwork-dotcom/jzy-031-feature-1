package engine

// fixedWindowMultiLua is the N-key atomic fixed window. All counters share the
// aligned current window; the first saturated key denies; otherwise all
// counters increment together.
//
// ARGV layout: n, [limit, window_seconds] x n, peek
const fixedWindowMultiLua = `
local n = tonumber(ARGV[1])
local peek = tonumber(ARGV[#ARGV])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local counts = {}
local wins = {}
local firstDeny = -1
for i = 1, n do
  local limit  = tonumber(ARGV[2 + (i-1)*2])
  local win_ms = tonumber(ARGV[3 + (i-1)*2]) * 1000
  local w = math.floor(now / win_ms) * win_ms
  local data = redis.call('HMGET', KEYS[i], 'w', 'c')
  local sw = tonumber(data[1]); local c = tonumber(data[2])
  if sw ~= w then c = 0 end
  counts[i] = c; wins[i] = win_ms
  if c >= limit and firstDeny == -1 then firstDeny = i end
end

local out = {}
if firstDeny ~= -1 then
  for i = 1, n do
    local limit = tonumber(ARGV[2 + (i-1)*2])
    local w = math.floor(now / wins[i]) * wins[i]
    local reset = (w + wins[i]) - now
    out[#out+1] = (i < firstDeny) and 1 or 0
    out[#out+1] = math.max(0, limit - counts[i])
    out[#out+1] = reset
  end
  out[#out+1] = firstDeny
  return out
end

for i = 1, n do
  local limit = tonumber(ARGV[2 + (i-1)*2])
  local w = math.floor(now / wins[i]) * wins[i]
  local c = counts[i] + 1
  redis.call('HSET', KEYS[i], 'w', w, 'c', c)
  redis.call('PEXPIRE', KEYS[i], wins[i] + 1000)
  out[#out+1] = 1
  out[#out+1] = limit - c
  out[#out+1] = (w + wins[i]) - now
end
out[#out+1] = 0
return out
`
