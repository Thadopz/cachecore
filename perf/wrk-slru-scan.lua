local counter = 0
local burst = tonumber(os.getenv("SLRU_SCAN_BURST") or "32")
local hotKeys = tonumber(os.getenv("HOT_KEYS") or "1")
local hotRepeats = tonumber(os.getenv("HOT_REPEATS") or "2")
local prefix = tostring(math.random(100000, 999999))

request = function()
  counter = counter + 1
  local slot = (counter - 1) % (burst + hotRepeats)
  local key
  if slot < hotRepeats then
    key = "key" .. tostring((counter + slot) % hotKeys)
  else
    key = "key" .. prefix .. tostring(1000000000 + counter)
  end
  return wrk.format("GET", "/api?key=" .. key)
end
