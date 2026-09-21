package engine

// leakyBucketMultiLua is the N-key atomic version of the leaky bucket.
// Pass 1 drains each queue and finds the first full one; if all have a slot,
// pass 2 enqueues on every level. The returned per-key reset_in_ms for an
// admitted request is its paced FIFO release delay.
//
// ARGV layout: n, [rate, cap] x n, peek
const leakyBucketMultiLua = `
local n = tonumber(ARGV[1])
local peek = tonumber(ARGV[#ARGV])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local levels = {}
local firstDeny = -1
for i = 1, n do
  local rate = tonumber(ARGV[2 + (i-1)*2])
  local cap  = tonumber(ARGV[3 + (i-1)*2])
  local data = redis.call('HMGET', KEYS[i], 'level', 'last')
  local lv = tonumber(data[1]); local last = tonumber(data[2])
  if lv == nil then lv = 0; last = now end
  local delta = math.max(0, now - last)
  local drained = delta * rate / 1000.0
  if drained >= lv then lv = 0 else lv = lv - drained end
  levels[i] = lv
  if lv > cap - 1 and firstDeny == -1 then firstDeny = i end
end

local out = {}
if firstDeny ~= -1 then
  for i = 1, n do
    local rate = tonumber(ARGV[2 + (i-1)*2])
    local cap  = tonumber(ARGV[3 + (i-1)*2])
    local allowed = (i < firstDeny) and 1 or 0
    local wait = math.ceil(math.max(0, levels[i] - cap + 1) / rate * 1000)
    local free = math.max(0, cap - math.ceil(levels[i]))
    out[#out+1] = allowed
    out[#out+1] = free
    out[#out+1] = wait
  end
  out[#out+1] = firstDeny
  return out
end

for i = 1, n do
  local rate = tonumber(ARGV[2 + (i-1)*2])
  local cap  = tonumber(ARGV[3 + (i-1)*2])
  local lv = levels[i] + 1
  redis.call('HSET', KEYS[i], 'level', lv, 'last', now)
  local ttl = math.ceil(lv / rate * 1000) + 1000
  redis.call('PEXPIRE', KEYS[i], ttl)
  local release = math.ceil(lv / rate * 1000)
  local free = math.max(0, cap - math.ceil(lv))
  out[#out+1] = 1
  out[#out+1] = free
  out[#out+1] = release
end
out[#out+1] = 0
return out
`
