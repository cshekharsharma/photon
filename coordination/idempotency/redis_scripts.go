package idempotency

const redisReserveScript = `
local key = KEYS[1]
local processing = ARGV[1]
local fingerprint = ARGV[2]
local ttl = tonumber(ARGV[3])

local current = redis.call("GET", key)
if not current then
	redis.call("PSETEX", key, ttl, processing)
	return {"reserved"}
end

local record = cjson.decode(current)
if record["f"] ~= fingerprint then
	return {"mismatch"}
end
if record["s"] == "processing" then
	return {"in_flight"}
end
if record["s"] == "completed" then
	return {"completed", current}
end
return {"mismatch"}
`

const redisCompleteScript = `
local key = KEYS[1]
local completed = ARGV[1]
local fingerprint = ARGV[2]
local owner = ARGV[3]
local fallbackTTL = tonumber(ARGV[4])

local current = redis.call("GET", key)
if not current then
	return {0, "missing"}
end

local record = cjson.decode(current)
if record["s"] ~= "processing" then
	return {0, "state"}
end
if record["f"] ~= fingerprint or record["o"] ~= owner then
	return {0, "mismatch"}
end

local ttl = redis.call("PTTL", key)
if ttl < 1 then
	ttl = fallbackTTL
end
redis.call("PSETEX", key, ttl, completed)
return {1, "ok"}
`

const redisReleaseScript = `
local key = KEYS[1]
local fingerprint = ARGV[1]
local owner = ARGV[2]

local current = redis.call("GET", key)
if not current then
	return 0
end

local record = cjson.decode(current)
if record["s"] == "processing" and record["f"] == fingerprint and record["o"] == owner then
	return redis.call("DEL", key)
end
return 0
`
