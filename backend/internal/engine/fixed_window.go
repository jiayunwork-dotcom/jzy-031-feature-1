package engine

// fixedWindowLua counts requests inside the current integer window.
//
// State: w = window start (epoch ms, aligned to window length), c = count.
//
// When the clock crosses a window boundary the counter resets to zero, so a
// client that exhausted the previous window can again spend the full budget
// instantly at the boundary — up to ~2x budget across the seam. That is the
// fixed window weakness that sliding window removes.
const fixedWindowLua = `
local limit = tonumber(ARGV[1])
local win_ms = tonumber(ARGV[2]) * 1000
local peek = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local w = math.floor(now / win_ms) * win_ms

local data = redis.call('HMGET', KEYS[1], 'w', 'c')
local sw = tonumber(data[1])
local c = tonumber(data[2])
if sw ~= w then
  c = 0
end

local allowed = 0
if c < limit then
  allowed = 1
  if peek == 0 then c = c + 1 end
end

if peek == 0 then
  redis.call('HSET', KEYS[1], 'w', w, 'c', c)
  redis.call('PEXPIRE', KEYS[1], math.ceil(win_ms / 1000) * 1000 + 1000)
end

local reset_ms = (w + win_ms) - now
return {allowed, limit - c, reset_ms}
`
