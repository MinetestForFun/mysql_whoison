local modname = minetest.get_current_modname()

local thismod = _G[modname]

minetest.register_chatcommand('seen', {
  param = "<name>",
  description = "Tells the last time a player was online",
  func = function (name, param)
    if param ~= nil then
      local t = thismod.get_last_online(param)
      if t ~= nil then
        local diff = (os.time() - t)
        minetest.chat_send_player(name,param.." was last online "..breakdowntime(diff).." ago")
      else
        minetest.chat_send_player(name,"Sorry, I have no record of "..param)
      end
    else
      minetest.chat_send_player(name,"Usage is /seen <name>")
    end
  end
})

minetest.register_chatcommand('timeonline', {
  param = "<name>",
  description = "Shows the cumulative time a player has been online",
  func = function (name, param)
    if param ~= nil then
      local t = thismod.get_time_online(param)
      if t ~= nil then
        minetest.chat_send_player(name,param.." has been online for "..breakdowntime(t))
      else
        minetest.chat_send_player(name,"Sorry, I have no record of "..param)
      end
    else
      minetest.chat_send_player(name,"Usage is /timeonline <name>")
    end
  end
})

function breakdowntime(t)
  local countdown = t
  local answer = ""

  if countdown >= 86400 then
    local days = math.floor(countdown / 86400)
    countdown = countdown % 86400
    answer = days .. " days "
  end
  if countdown >= 3600 then
    local hours = math.floor(countdown / 3600)
    countdown = countdown % 3600
    answer = answer .. hours .. " hours "
  end
  if countdown >= 60 then
    local minutes = math.floor(countdown / 60)
    countdown = countdown % 60
    answer = answer .. minutes .. " minutes "
  end

  local seconds = countdown
  answer = answer .. seconds .. " seconds"

  return answer
end
