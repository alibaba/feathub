-- Copyright 2022 The Feathub Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--[[
This script is used to write Feathub features to a Redis database. The features
are persisted to Redis as structured map objects.

Feathub features written out with this script must provide timestamp values.
The timestamp value must be an 8-byte array that represents a long value saved
in big-endian order. The long value represents the milliseconds that have passed
since the epoch. If two features with the same key are written to the same Redis
database, the one with a larger timestamp value would finally be persisted to
Redis.
--]]

local function bytes_to_long_big_endian(string_bytes_array)
    local long_value = 0
    if string_bytes_array == false then
        long_value = 0
    else
        for i = 1, #string_bytes_array do
            long_value = long_value * 0x100 + string.byte(string_bytes_array, i)
        end
    end
    return long_value
end

local key = ARGV[1]
local timestamp_field_name = '__timestamp__'
local current_timestamp_str = redis.call('HGET', key, timestamp_field_name)
local current_timestamp = bytes_to_long_big_endian(current_timestamp_str)
local new_timestamp_str = ARGV[2]
local new_timestamp = bytes_to_long_big_endian(new_timestamp_str)
if new_timestamp > current_timestamp then
  for i=3,table.getn(ARGV),2 do
    redis.call('HSET', key, ARGV[i], ARGV[i + 1])
  end
  redis.call('HSET', key, timestamp_field_name, new_timestamp_str)
end;
