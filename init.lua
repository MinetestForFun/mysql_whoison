local modname = minetest.get_current_modname()
local modpath = minetest.get_modpath(modname) 

local thismod = {}
_G[modname] = thismod

local LogI = mysql_base.mklog('action', modname)
local LogE = mysql_base.mklog('error', modname)

if not mysql_auth.enabled then
  LogI("mysql_auth is disabled, not enabling mod")
  return
end

local conn = mysql_base.conn

do
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
        timeonline_type = 'BIGINT'
      }
    }
  }
  do -- Tables and schema settings & default values
    tables.whoison_logs.name = get('tables.whoison_logs.name') or 'whoison_logs'
    local S = tables.whoison_logs.schema
    S.login = get('tables.whoison_logs.schema.login') or 'login'
    S.login_type = get('tables.whoison_logs.schema.login_type') or 'BIGINT'
    S.logout = get('tables.whoison_logs.schema.logout') or 'logout'
    S.logout_type = get('tables.whoison_logs.schema.logout_type') or 'BIGINT'
    S.ip = get('tables.whoison_logs.schema.ip') or 'ip'
    S.ip_type = get('tables.whoison_logs.schema.ip_type') or 'VARCHAR(39)'
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
        {S.ip, S.ip_type, notnull = true},
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
    {S.logout, S.logout_type},
    {S.ip, S.ip_type},
  })

  -- Totals table existence check and setup
  if not mysql_base.table_exists(tables.whoison_totals.name) then
    -- Totals table doesn't exist, create it
    local S = tables.whoison_totals.schema
    mysql_base.create_table(tables.whoison_totals.name, {
      columns = {
        {S.userid, S.userid_type, notnull = true},
        {S.timeonline, S.timeonline_type, notnull = true},
      },
      pkey = {S.userid},
      fkeys = {
        [S.userid] = {
          table = mysql_auth.tables.auths.name,
          column = mysql_auth.tables.auths.schema.userid,
        },
      },
    })
    LogI("Created table '" .. mysql_base.dbname .. "." .. tables.whoison_logs.name .. "'")
  end
  S = tables.whoison_totals.schema
  local log_connection_stmt, log_connection_params = mysql_base.prepare_insert(
    tables.whoison_totals.name, {
    {S.userid, S.userid_type},
    {S.timeonline, S.timeonline_type},
  })
  
  
end
