package engine

// tokenBucketLua implements the classic token bucket.
//
// State: tokens = current token count (may be fractional, capped at capacity),
// last = last refill time in ms.
//
// Tokens are added at a constant rate (rate/s) up to capacity; an idle bucket
// therefore holds `capacity` tokens, which an arriving burst can consume
// instantly. Anything beyond capacity is discarded.
const tokenBucketLua = `
local rate = tonumber(ARGV[1])
local cap = tonumber(ARGV[2])
local peek = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local data = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens = tonumber(data[1])
local last = tonumber(data[2])
if tokens == nil then
  tokens = cap
  last = now
end

local delta = math.max(0, now - last)
tokens = math.min(cap, tokens + delta * rate / 1000.0)

local allowed = 0
if tokens >= 1 then
  allowed = 1
  if peek == 0 then tokens = tokens - 1 end
end

if peek == 0 then
  redis.call('HSET', KEYS[1], 'tokens', tokens, 'last', now)
  local ttl = math.ceil((cap - tokens) / rate * 1000) + 1000
  redis.call('PEXPIRE', KEYS[1], ttl)
end

local full_ms = 0
if tokens < cap then
  full_ms = math.ceil((cap - tokens) / rate * 1000)
end
return {allowed, math.floor(tokens), full_ms}
`
