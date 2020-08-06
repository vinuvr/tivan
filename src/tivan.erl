%%%-------------------------------------------------------------------
%%% @author Chaitanya Chalasani
%%% @copyright (C) 2019, ArkNode.IO
%%% @doc
%%%
%%% @end
%%% Created : 2019-04-19 18:38:31.676004
%%%-------------------------------------------------------------------
-module(tivan).
-export([create/1
        ,create/2
        ,drop/1
        ,clear/1
        ,backup/0
        ,backup/1
        ,restore/0
        ,restore/1
        ,inspect_backup/0
        ,inspect_backup/1
        ,is_local/1
        ,info/0
        ,info/1
        ,info/2
        ,put/2
        ,put/3
        ,get/1
        ,get/2
        ,get_last_key/1
        ,remove/2
        ,remove/3]).

create(Table) ->
  tivan_schema:create(Table).

create(Table, Options) ->
  tivan_schema:create(Table, Options).

drop(Table) ->
  tivan_schema:drop(Table).

clear(Table) ->
  tivan_schema:clear(Table).

is_local(Table) ->
  tivan_schema:is_local(Table).

backup() ->
  tivan_schema:backup().

backup(Options) ->
  tivan_schema:backup(Options).

restore() ->
  tivan_schema:restore().

restore(Options) ->
  tivan_schema:restore(Options).

inspect_backup() ->
  tivan_schema:inspect_backup().

inspect_backup(BackupFilename) ->
  tivan_schema:inspect_backup(BackupFilename).

info() ->
  tivan_schema:info().

info(Table) ->
  tivan_schema:info(Table).

info(Table, Item) ->
  tivan_schema:info(Table, Item).

put(Table, ObjectOrObjects) ->
  tivan_mnesia:put(Table, ObjectOrObjects).

put(Table, ObjectOrObjects, Options) ->
  tivan_mnesia:put(Table, ObjectOrObjects, Options).

get(Table) ->
  tivan_mnesia:get(Table).

get(Table, Options) ->
  tivan_mnesia:get(Table, Options).

get_last_key(Table) ->
  tivan_mnesia:get_last_key(Table).

remove(Table, ObjectOrObjects) ->
  tivan_mnesia:remove(Table, ObjectOrObjects).

remove(Table, ObjectOrObjects, Options) ->
  tivan_mnesia:remove(Table, ObjectOrObjects, Options).
