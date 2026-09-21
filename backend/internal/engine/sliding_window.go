package engine

// slidingWindowLua is a weighted sliding window over two adjacent fixed
// windows.
//
// State: a = start ms of the current window, ca = count in current window,
// cb = count in the previous window.
//
// The effective count for the trailing window of length W is:
//
//	prev_count * (1 - elapsed_in_current / W) + current_count
//
// At a boundary the previous window's count is linearly faded out instead of
// being dropped instantly, so the ~2x boundary burst of the fixed window is
// impossible. It approximates the sliding-log result with O(1) state.
const slidingWindowLua = `
local limit = tonumber(ARGV[1])
local win_ms = tonumber(ARGV[2]) * 1000
local peek = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local cur = math.floor(now / win_ms) * win_ms
local elapsed = now - cur

local data = redis.call('HMGET', KEYS[1], 'a', 'ca', 'cb')
local a = tonumber(data[1])
local ca = tonumber(data[2])
local cb = tonumber(data[3])

if a == nil then
  a = cur; ca = 0; cb = 0
elseif a == cur - win_ms then
  cb = ca; ca = 0
elseif a ~= cur then
  cb = 0; ca = 0
end
a = cur

local weight = math.max(0, (win_ms - elapsed) / win_ms)
local count = cb * weight + ca
local allowed = 0
if count < limit then
  allowed = 1
  if peek == 0 then ca = ca + 1 end
end

if peek == 0 then
  redis.call('HSET', KEYS[1], 'a', a, 'ca', ca, 'cb', cb)
  redis.call('PEXPIRE', KEYS[1], 2 * win_ms + 1000)
end

local reset_ms = win_ms - elapsed
return {allowed, math.max(0, limit - math.ceil(ca + cb * weight)), reset_ms}
`
