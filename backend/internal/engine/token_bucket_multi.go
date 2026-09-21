package engine

// tokenBucketMultiLua atomically evaluates the token bucket over N keys (the
// four quota levels of one rule). It performs two passes inside the single
// atomic script: first read every bucket and find the first key that cannot
// serve a token; only if ALL keys admit does the second pass consume from all
// of them. This removes the peek-then-try TOCTOU race when several gateway
// goroutines/instances contend on the same rule.
//
// ARGV layout: n, [rate, cap] x n, peek
const tokenBucketMultiLua = `
local n = tonumber(ARGV[1])
local peek = tonumber(ARGV[#ARGV])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

-- pass 1: compute current token count per key
local tokens = {}
local firstDeny = -1
for i = 1, n do
  local rate = tonumber(ARGV[2 + (i-1)*2])
  local cap  = tonumber(ARGV[3 + (i-1)*2])
  local data = redis.call('HMGET', KEYS[i], 'tokens', 'last')
  local tk = tonumber(data[1]); local last = tonumber(data[2])
  if tk == nil then tk = cap; last = now end
  local delta = math.max(0, now - last)
  tk = math.min(cap, tk + delta * rate / 1000.0)
  tokens[i] = tk
  if tk < 1 and firstDeny == -1 then firstDeny = i end
end

local out = {}
if firstDeny ~= -1 then
  -- deny: report the first failing level, mutate nothing
  for i = 1, n do
    local rate = tonumber(ARGV[2 + (i-1)*2])
    local cap  = tonumber(ARGV[3 + (i-1)*2])
    local allowed = (i < firstDeny) and 1 or 0
    local full_ms = 0
    if tokens[i] < cap then full_ms = math.ceil((cap - tokens[i]) / rate * 1000) end
    out[#out+1] = allowed
    out[#out+1] = math.floor(tokens[i])
    out[#out+1] = full_ms
  end
  out[#out+1] = firstDeny
  return out
end

-- pass 2: all admitted, consume one token from each
for i = 1, n do
  local rate = tonumber(ARGV[2 + (i-1)*2])
  local cap  = tonumber(ARGV[3 + (i-1)*2])
  local tk = tokens[i] - 1
  redis.call('HSET', KEYS[i], 'tokens', tk, 'last', now)
  local ttl = math.ceil((cap - tk) / rate * 1000) + 1000
  redis.call('PEXPIRE', KEYS[i], ttl)
  local full_ms = 0
  if tk < cap then full_ms = math.ceil((cap - tk) / rate * 1000) end
  out[#out+1] = 1
  out[#out+1] = math.floor(tk)
  out[#out+1] = full_ms
end
out[#out+1] = 0
return out
`
