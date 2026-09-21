package engine

// slidingLogLua keeps the exact timestamp of every request still inside the
// trailing window.
//
// State: a ZSET whose members are unique nonces and scores are request times
// in ms. On each call expired entries (score < now - window) are evicted and
// the remaining cardinality is compared with the limit. This is exact rather
// than weighted: no smoothing approximation.
const slidingLogLua = `
local limit = tonumber(ARGV[1])
local win_ms = tonumber(ARGV[2]) * 1000
local nonce = ARGV[3]
local peek = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local cutoff = now - win_ms

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
local count = redis.call('ZCARD', KEYS[1])

local allowed = 0
if count < limit then
  allowed = 1
  if peek == 0 then redis.call('ZADD', KEYS[1], now, nonce) end
end

if peek == 0 then
  redis.call('PEXPIRE', KEYS[1], math.ceil(win_ms / 1000) * 1000 + 1000)
end

local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local reset_ms = 0
if oldest[2] ~= nil then
  reset_ms = (tonumber(oldest[2]) + win_ms) - now
  if reset_ms < 0 then reset_ms = 0 end
end
return {allowed, math.max(0, limit - count), reset_ms}
`
