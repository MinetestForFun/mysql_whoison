local modname = minetest.get_current_modname()
local modpath = minetest.get_modpath(modname)

local thismod = {}
_G[modname] = thismod

local LogI = mysql_base.mklog('action', modname)
local LogE = mysql_base.mklog('error', modname)
local LogV = function() end --mysql_base.mklog('action', modname)

if not mysql_auth.enabled then
  LogI("mysql_auth is disabled, not enabling mod")
  return
end

local conn = mysql_base.conn

thismod.DISCONNECT_REASON = {
  UNKNOWN = 0,
  PLAYER_LEAVE = 1,
  PLAYER_CRASH = 2,
  PLAYER_TIMEOUT = 3,
  KICK = 4,
  BAN = 5,
  SERVER_SHUTDOWN = 100,
  SERVER_CRASH = 101
}

thismod.logentrymap = {}
thismod.jointime = {}

local get = mysql_base.mkget(modname)

local tables = {
  whoison_logs = {
    schema = {
      id = 'id',
      id_type = 'INT',
      userid = mysql_auth.tables.auths.schema.userid,
      userid_type = mysql_auth.tables.auths.schema.userid_type
    }
  },
  whoison_totals = {
    name = 'whoison_totals',
    schema = {
      userid = mysql_auth.tables.auths.schema.userid,
      userid_type = mysql_auth.tables.auths.schema.userid_type,
      timeonline = 'timeonline',
      timeonline_type = 'BIGINT',
      lastonline = 'lastonline',
      lastonline_type = 'BIGINT'
    }
  }
}
do -- Tables and schema settings & default values
  tables.whoison_logs.name = get('tables.whoison_logs.name') or 'whoison_logs'
  local S, SN = tables.whoison_logs.schema, 'tables.whoison_logs.schema.'
  S.login = get(SN..'login') or 'login'
  S.login_type = get(SN..'login_type') or 'BIGINT'
  S.logout = get(SN..'logout') or 'logout'
  S.logout_type = get(SN..'logout_type') or 'BIGINT'
  S.lastrecord = get(SN..'lastrecord') or 'lastrecord'
  S.lastrecord_type = get(SN..'lastrecord_type') or 'BIGINT'
  -- ^ Last recorded online time
  S.committed = get(SN..'committed') or 'committed'
  S.committed_type = get(SN..'committed_type') or 'BOOLEAN'
  -- ^ Log entry finalized and time counted toward total
  S.logout_reason = get(SN..'logout_reason') or 'logout_reason'
  S.logout_reason_type = get(SN..'logout_reason') or 'SMALLINT UNSIGNED'
  -- ^ Disconnect reason enum
  S.ip = get(SN..'ip') or 'ip'
  S.ip_type = get(SN..'ip_type') or 'VARCHAR(39)'
  -- ^ Defaults to length(full IPv6 adress as string) + 1
  S.port = get(SN..'port') or 'port'
  S.port_type = get(SN..'port_type') or 'SMALLINT UNSIGNED'
  -- ^ Unsigned smallint = uint16_t, range [0, 65535] = port range
end


-- Logs table existence check and setup
if not mysql_base.table_exists(tables.whoison_logs.name) then
  -- Logs table doesn't exist, create it
  local S = tables.whoison_logs.schema
  mysql_base.create_table(tables.whoison_logs.name, {
    columns = {
      {S.id, S.id_type, notnull = true, autoincrement = true},
      {S.userid, S.userid_type, notnull = true},
      {S.login, S.login_type, notnull = true},
      {S.logout, S.logout_type},
      {S.lastrecord, S.lastrecord_type, notnull = true},
      {S.committed, S.committed_type, notnull = true, default = '0'},
      {S.logout_reason, S.logout_reason_type},
      {S.ip, S.ip_type, notnull = true},
      {S.port, S.port_type, notnull = true},
    },
    pkey = {S.id},
    fkeys = {
      [S.userid] = {
        table = mysql_auth.tables.auths.name,
        column = mysql_auth.tables.auths.schema.userid,
      },
    },
  })
  LogI("Created table '" .. mysql_base.dbname .. "." .. tables.whoison_logs.name .. "'")
end
local S = tables.whoison_logs.schema
local log_connection_stmt, log_connection_params = mysql_base.prepare_insert(
  tables.whoison_logs.name, {
  {S.userid, S.userid_type},
  {S.login, S.login_type},
  {S.lastrecord, S.lastrecord_type},
  {S.ip, S.ip_type},
})
local close_connection_stmt, close_connection_params = mysql_base.prepare_update(
  tables.whoison_logs.name, {
  {S.logout, S.logout_type},
  {S.lastrecord, value = 'NULL'},
  {S.logout_reason, S.logout_reason_type},
  {S.committed, value = 1},
}, S.id .. '=?', {S.id_type})
local update_record_stmt, update_record_params = mysql_base.prepare_update(
  tables.whoison_logs.name, {
  {S.lastrecord, S.lastrecord_type},
}, S.id .. '=?', {S.id_type})
-- Log table fixing
local fix_connection_stmt, fix_connection_params = mysql_base.prepare_update(
  tables.whoison_logs.name, {
  {S.logout, value = S.lastrecord},
  {S.lastrecord, value = 'NULL'},
  {S.logout_reason, S.logout_reason_type},
  {S.committed, value = 1},
}, S.id .. '=?', {S.id_type})
local _, select_uncommitted_params, _ =
  mysql_base.prepare_select(
  tables.whoison_logs.name, {
  {S.id, S.id_type},
  {S.userid, S.userid_type},
}, 'committed=0')

-- Totals table existence check and setup
if not mysql_base.table_exists(tables.whoison_totals.name) then
  -- Totals table doesn't exist, create it
  S = tables.whoison_totals.schema
  mysql_base.create_table(tables.whoison_totals.name, {
    columns = {
      {S.userid, S.userid_type, notnull = true},
      {S.timeonline, S.timeonline_type, notnull = true},
      {S.lastonline, S.lastonline_type, notnull = true, default = '0'},
    },
    pkey = {S.userid},
    fkeys = {
      [S.userid] = {
        table = mysql_auth.tables.auths.name,
        column = mysql_auth.tables.auths.schema.userid,
      },
    },
  })
  LogI("Created table '" .. mysql_base.dbname .. "." .. tables.whoison_totals.name .. "'")
end
S = tables.whoison_totals.schema
local insert_zerototal_stmt = conn:prepare(
  'INSERT IGNORE INTO ' .. tables.whoison_totals.name .. '(userid, timeonline) VALUES(?,0)')
local insert_zerototal_params = insert_zerototal_stmt:bind_params({S.userid_type})
local update_total_stmt, update_total_params = mysql_base.prepare_update(
  tables.whoison_totals.name, {
  {S.timeonline, S.timeonline_type, value = S.timeonline..'+?'},
  {S.lastonline, S.lastonline_type, value = 'greatest('..S.lastonline..',?)'},
}, S.userid .. '=?', {S.userid_type})
local get_total_stmt, get_total_params, get_total_results = mysql_base.prepare_select(
  tables.whoison_totals.name, {
  {S.timeonline, S.timeonline_type},
  {S.lastonline, S.lastonline_type},
}, S.userid .. '=?', {S.userid_type})


local function db_get_totals(name)
  local auth = minetest.get_auth_handler().get_auth(name)
  get_total_params:set(1, auth.userid)
  get_total_stmt:exec()
  get_total_stmt:store_result()
  if not get_total_stmt:fetch() then
    LogI("Failed to get totals for " .. name .. ": " .. msg)
    return nil
  end
  local time, lastonline = get_total_results:get(1), get_total_results:get(2)
  get_total_stmt:free_result()
  return { tonumber(time), tonumber(lastonline) }
end

function thismod.get_time_online_database(name)
  return db_get_totals(name)[1]
end

function thismod.get_time_online(name)
  local dbtime = thismod.get_time_online_database(name)
  if dbtime == nil or thismod.jointime[name] == nil then
    return nil
  end
  return dbtime + math.floor(os.time()) - thismod.jointime[name]
end

function thismod.update_lastrecord_time()
  LogV("Updating lastrecord times")
  conn:query('START TRANSACTION')
  update_record_params:set(1, math.floor(os.time()))
  for _, player in ipairs(minetest.get_connected_players()) do
    local name = player:get_player_name()
    update_record_params:set(2, thismod.logentrymap[name][2])
    local success, msg = pcall(update_record_stmt.exec, update_record_stmt)
    if not success then
      LogE("Failed to update " .. name .. " logged-in record: " .. msg)
      conn:rollback()
      error(msg)
    end
    if update_record_stmt:affected_rows() ~= 1 then
      LogE("Failed to update " .. name .. " logged-in record: affected row count is " ..
            update_record_stmt:affected_rows() .. ", expected 1")
      conn:rollback()
      error(msg)
    end
  end
  conn:commit()
end

function thismod.get_last_online_database(name)
  return db_get_totals(name)[2]
end

function thismod.get_last_online(name)
  local dbtime = thismod.get_time_online_database(name)
  if dbtime == nil then
    return nil
  end
  if thismod.jointime[name] ~= nil then
    return math.floor(os.time())
  end
  return dbtime
end


minetest.register_on_joinplayer(function (player)
  local name = player:get_player_name()
  LogV("Logging player join: " .. name)
  local auth = minetest.get_auth_handler().get_auth(name)
  local pinfo = minetest.get_player_information(name)
  local now = math.floor(os.time())

  insert_zerototal_params:set(1, auth.userid)
  insert_zerototal_stmt:exec()

  log_connection_params:set(1, auth.userid)
  log_connection_params:set(2, now)
  log_connection_params:set(3, now)
  log_connection_params:set(4, pinfo.address)
  -- log_connection_params:set(5, pinfo.port) -- If only...
  local success, msg = pcall(log_connection_stmt.exec, log_connection_stmt)
  if not success then
    LogE("Failed to record " .. name .. " login: " .. msg)
    error(msg)  -- Bail out, we don't handle cases of DB errors
  end
  if log_connection_stmt:affected_rows() ~= 1 then
    LogE("Failed to record " .. name .. " login: affected row count is " ..
          log_connection_stmt:affected_rows() .. ", expected 1")
    error(msg)
  end
  local id = tonumber(conn:insert_id())
  thismod.logentrymap[name] = { auth.userid, id }
  thismod.jointime[name] = now
  LogV("Player join logged: " .. name .. " -> " .. id)
end)

function thismod.log_player_disconnect(name, reason, transact, fixid, playerid)
  local id
  local params, stmt = close_connection_params, close_connection_stmt
  local now = math.floor(os.time())
  if fixid then
    id = fixid
    params, stmt = fix_connection_params, fix_connection_stmt
    LogV("Fixing player leave: " .. name .. " -> " .. id .. " reason=" .. reason)
  else
    playerid = thismod.logentrymap[name][1]
    id = thismod.logentrymap[name][2]
    LogV("Logging player leave: " .. name .. " -> " .. id .. " reason=" .. reason)
  end
  if transact then conn:query('START TRANSACTION') end
  if fixid then
    params:set(1, reason)
    params:set(2, id)
  else
    params:set(1, now)
    params:set(2, reason)
    params:set(3, id)
  end
  local success, msg = pcall(stmt.exec, stmt)
  if not success then
    LogE("Failed to record " .. name .. " logout: " .. msg)
    conn:rollback()
    error(msg)
  end
  if stmt:affected_rows() ~= 1 then
    LogE("Failed to record " .. name .. " logout: affected row count is " ..
          stmt:affected_rows() .. ", expected 1")
    conn:rollback()
    error(msg)
  end

  local timedelta, disconnect
  if fixid then
    conn:query('SELECT login, logout FROM ' .. tables.whoison_logs.name .. ' WHERE ' ..
        tables.whoison_logs.schema.id .. '=' .. fixid)
    local res = conn:store_result()
    local row = res:fetch('n')
    disconnect = tonumber(row[2])
    timedelta = tonumber(row[2] - row[1])
  else
    disconnect = now
    timedelta = now - thismod.jointime[name]
  end
  if timedelta < 1 then timedelta = 1 end
  LogV("timedelta = " .. timedelta)

  update_total_params:set(1, timedelta)
  update_total_params:set(2, disconnect)
  update_total_params:set(3, playerid)
  success, msg = pcall(update_total_stmt.exec, update_total_stmt)
  if not success then
    LogE("Failed updating " .. name .. " totals: " .. msg)
    conn:rollback()
    error(msg)
  end
  if update_total_stmt:affected_rows() ~= 1 then
    LogE("Failed updating " .. name .. " totals: affected row count is " ..
          update_total_stmt:affected_rows() .. ", expected 1")
    conn:rollback()
    error(msg)
  end

  if transact then conn:commit() end
end

-- Integrity checking
-- The game may have crashed leaving data/totals uncommitted, clear up the situation
do
  select_uncommitted_stmt:exec()
  select_uncommitted_stmt:store_result()
  local list = {}
  while select_uncommitted_stmt:fetch() ~= false do
    table.insert(list, {select_uncommitted_result:get(1),select_uncommitted_result:get(2)})
  end
  select_uncommitted_stmt:free_result()
  select_uncommitted_stmt:close()
  LogV("Fixing " .. #list .. " log entries")
  conn:query('START TRANSACTION')
  for i = 1, #list do
    local id, playerid = unpack(list[i])
    thismod.log_player_disconnect('<#'..playerid..'>', thismod.DISCONNECT_REASON.SERVER_CRASH,
        false, id, playerid)
  end
  conn:commit()
  -- Done with the variables, clean up so the GC runs the destructors
  fix_connection_stmt, fix_connection_params, select_uncommitted_stmt,
      select_uncommitted_params, select_uncommitted_result = nil, nil, nil, nil, nil
end

minetest.register_on_leaveplayer(function (player)
  -- Want to know why the player leaves? MT's code quality speaks for itself: you can't.
  -- None of the dozen core devs had the idea to have a "reason" param here in 7 years.
  local name = player:get_player_name()
  thismod.log_player_disconnect(name, thismod.DISCONNECT_REASON.PLAYER_LEAVE, true)
end)

mysql_base.register_on_shutdown(function ()
  conn:query('START TRANSACTION')
  for name, _ in pairs(thismod.logentrymap) do
    LogV("Shutdown: " .. name)
    thismod.log_player_disconnect(name, thismod.DISCONNECT_REASON.SERVER_SHUTDOWN)
  end
  conn:commit()
end)

dofile(modpath .. '/interface.lua')

local interval = 150
local function lastrecord_loop()
  thismod.update_lastrecord_time()
  minetest.after(interval, lastrecord_loop)
end
minetest.after(interval, lastrecord_loop)
