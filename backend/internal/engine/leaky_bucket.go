package engine

// leakyBucketLua implements the leaky bucket as a shaping queue.
//
// State: level = queued water (fractional slots), last = last leak time in ms.
//
// Water leaks out at one constant rate (rate/s). An arriving request is
// rejected when the queue is full (no free slot); accepted requests join the
// queue and are *released* in FIFO order at the leak rate. Unlike the token
// bucket, idle time never accumulates release credit above the queue drain
// schedule, so a burst of accepted requests exits as a smooth, evenly spaced
// stream rather than all at once.
//
// Returned reset_in_ms for an accepted request is ITS release delay (the
// time until this request leaks out); for a rejected one it is the wait until
// the next slot frees. The caller records allow-throughput at release time,
// which is what makes the shaped output curve smooth.
const leakyBucketLua = `
local rate = tonumber(ARGV[1])
local cap = tonumber(ARGV[2])
local peek = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local data = redis.call('HMGET', KEYS[1], 'level', 'last')
local level = tonumber(data[1])
local last = tonumber(data[2])
if level == nil then
  level = 0
  last = now
end

local delta = math.max(0, now - last)
local drained = delta * rate / 1000.0
if drained >= level then
  level = 0
else
  level = level - drained
end

-- A whole discrete slot must be free for admission.
local allowed = 0
local release_ms = 0
if level <= cap - 1 then
  allowed = 1
  if peek == 0 then level = level + 1 end
  -- FIFO release time of the request just enqueued.
  release_ms = math.ceil(level / rate * 1000)
end

if peek == 0 then
  redis.call('HSET', KEYS[1], 'level', level, 'last', now)
  local ttl = math.ceil(level / rate * 1000) + 1000
  redis.call('PEXPIRE', KEYS[1], ttl)
end

local wait_ms = release_ms
if allowed == 0 then
  -- wait until one slot drains
  wait_ms = math.ceil(math.max(0, level - cap + 1) / rate * 1000)
end
local free = math.max(0, cap - math.ceil(level))
return {allowed, free, wait_ms}
`
