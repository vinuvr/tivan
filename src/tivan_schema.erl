%%%-------------------------------------------------------------------
%%% @author Chaitanya Chalasani
%%% @copyright (C) 2018, ArkNode.IO
%%% @doc
%%%
%%% @end
%%% Created : 2018-08-08 10:26:55.093338
%%%-------------------------------------------------------------------
-module(tivan_schema).

-behaviour(gen_server).

-define(BACKUP_FILE, "tivan.backup").

%% API
-export([start_link/0
        ,create/1
        ,create/2
        ,drop/1
        ,clear/1
        ,is_local/1
        ,backup/0
        ,backup/1
        ,restore/0
        ,restore/1
        ,inspect_backup/0
        ,inspect_backup/1
        ,info/0
        ,info/1
        ,info/2]).

%% gen_server callbacks
-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3]).

-ignore_xref([start_link/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create(Table) ->
  create(Table, #{memory => true, persist => true}).

create(Table, Options) when is_atom(Table), is_map(Options) ->
  gen_server:call(?MODULE, {create, Table, Options}, 60000).

drop(Table) when is_atom(Table) ->
  gen_server:cast(?MODULE, {drop, Table}).

clear(Table) when is_atom(Table) ->
  gen_server:call(?MODULE, {clear, Table}).

is_local(Table) ->
  lists:member(Table, mnesia:system_info(local_tables)).

backup() ->
  backup(#{}).

backup(Options) ->
  gen_server:call(?MODULE, {backup, Options}, infinity).

restore() ->
  restore(#{}).

restore(Options) ->
  gen_server:call(?MODULE, {restore, Options}, infinity).

inspect_backup() ->
  BackupFilename = application:get_env(backup_file, tivan, ?BACKUP_FILE),
  inspect_backup(BackupFilename).

inspect_backup(BackupFilename) ->
  do_inspect_backup(BackupFilename).

info() -> mnesia:info().

info(Table) -> mnesia:table_info(Table, all).

info(Table, ItemList) when is_list(ItemList) ->
  [ {Item, info(Table, Item)} || Item <- ItemList ];
info(Table, Item) -> mnesia:table_info(Table, Item).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
  init_schema(),
  {ok, #{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({create, Table, Options}, _From, State) ->
  Reply = do_create(Table, Options),
  {reply, Reply, State};
handle_call({clear, Table}, _From, State) ->
  Reply = do_clear(Table),
  {reply, Reply, State};
handle_call({backup, Options}, _From, State) ->
  Reply = do_backup(Options),
  {reply, Reply, State};
handle_call({restore, Options}, _From, State) ->
  Reply = do_restore(Options),
  {reply, Reply, State};
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({drop, Table}, State) ->
  do_drop(Table),
  {noreply, State};
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_schema() ->
  case application:get_env(tivan, remote_node, undefined) of
    undefined ->
      init_standalone_schema();
    RemoteNode ->
      case net_adm:ping(RemoteNode) of
        pang ->
          init_standalone_schema();
        pong ->
          init_cluster_schema(RemoteNode)
      end
  end.

init_cluster_schema(RemoteNode) ->
  MnesiaNodes = mnesia:system_info(running_db_nodes),
  case lists:member(RemoteNode, MnesiaNodes) of
    true ->
      init_standalone_schema();
    false ->
      rpc:call(RemoteNode, mnesia, change_config,
               [extra_db_nodes, [node()]]),
      init_standalone_schema()
  end.

init_standalone_schema() ->
  PersistFlag = application:get_env(tivan, persist_db, true),
  case mnesia:table_info(schema, storage_type) of
    disc_copies when not PersistFlag ->
      vaporize();
    ram_copies when PersistFlag ->
      persist();
    _ ->
      ok
  end.

vaporize() ->
  LocalTables = mnesia:system_info(local_tables) -- [schema],
  Vaporize = fun(Table) ->
                 case mnesia:table_info(Table, storage_type) of
                   disc_copies ->
                     catch mnesia:change_table_copy_type(Table, node(), ram_copies);
                   _ ->
                     ok
                 end
             end,
  lists:map(Vaporize, LocalTables),
  mnesia:change_table_copy_type(schema, node(), ram_copies).

persist() ->
  mnesia:change_table_copy_type(schema, node(), disc_copies).

do_create(Table, Options) ->
  SchemaPersistFlag = application:get_env(tivan, persist_db, true),
  MnesiaOptions = maps:get(mnesia_options, Options, []),
  {Attributes, Indexes} = get_attributes_indexes(Options),
  Memory = maps:get(memory, Options, true),
  Persist = maps:get(persist, Options, true),
  Type = maps:get(type, Options, set),
  StorageRequest = if Memory, Persist, SchemaPersistFlag -> disc_copies;
                      Persist, SchemaPersistFlag -> disc_only_copies;
                      Memory -> ram_copies;
                      true -> remote_copies
                   end,
  StorageType = case catch mnesia:table_info(Table, storage_type) of
                  {'EXIT', _Reason} -> not_found;
                  unknown -> remote_copies;
                  St -> St
                end,
  case {StorageType, StorageRequest} of
    {not_found, remote_copies} -> {error, not_found};
    {not_found, _} -> create_table(Table, Attributes, Indexes, Type
                                  ,StorageRequest, MnesiaOptions, Options);
    {remote_copies, remote_copies} -> ok;
    {remote_copies, _} -> copy_table(Table, Attributes, Indexes, StorageRequest, Options);
    {_, remote_copies} -> remove_local(Table);
    {StorageRequest, _} -> post_create(Table, Attributes, Indexes, Options);
    {_, _} -> change_storage(Table, Attributes, Indexes, StorageRequest, Options)
  end.

create_table(Table, Attributes, Indexes, Type, StorageRequest, MnesiaOptions, Options) ->
  case mnesia:create_table(Table, [{attributes, Attributes}, {index, Indexes}, {type, Type},
                                   {StorageRequest, [node()]}|MnesiaOptions]) of
    {atomic, ok} ->
      post_create(Table, Attributes, Indexes, Options);
    Error -> Error
  end.

copy_table(Table, Attributes, Indexes, StorageRequest, Options) ->
  case mnesia:add_table_copy(Table, node(), StorageRequest) of
    {atomic, ok} ->
      post_create(Table, Attributes, Indexes, Options);
    Error -> Error
  end.

remove_local(Table) ->
  case mnesia:del_table_copy(Table, node()) of
    {atomic, ok} ->
      ok;
    Error -> Error
  end.

change_storage(Table, Attributes, Indexes, StorageRequest, Options) ->
  case mnesia:change_table_copy_type(Table, node(), StorageRequest) of
    {atomic, ok} ->
      post_create(Table, Attributes, Indexes, Options);
    Error -> Error
  end.

post_create(Table, Attributes, Indexes, Options) ->
  wait_for_tables([Table], 50000),
  transform_if_needed(Table, Attributes, Indexes, Options),
  ok.

get_attributes_indexes(#{columns := AttributesIndexes}) ->
  lists:foldr(
    fun({Attribute}, {AttributesAcc, IndexesAcc}) ->
        {[Attribute|AttributesAcc], [Attribute|IndexesAcc]};
       (Attribute, {AttributesAcc, IndexesAcc}) ->
        {[Attribute|AttributesAcc], IndexesAcc}
    end,
    {[], []},
    AttributesIndexes
   );
get_attributes_indexes(_Other) ->
  {[key, value], []}.

wait_for_tables(Tables, Time) ->
  case mnesia:wait_for_tables(Tables, Time) of
    {timeout, TablesToLoad} ->
      TimeToWait = if Time > 5000 -> Time div 2; true -> Time end,
      wait_for_tables(TablesToLoad, TimeToWait);
    {error, Reason} ->
      exit(Reason);
    ok ->
      ok
  end.

transform_if_needed(Table, Attributes, Indexes, Options) ->
  TransformFlag = maps:get(transform, Options, true),
  case mnesia:table_info(Table, attributes) of
    Attributes ->
      IndexesExisting = [ lists:nth(X-1, Attributes)
                          || X <- mnesia:table_info(Table, index) ],
      IndexesToDelete = IndexesExisting -- Indexes,
      [ mnesia:del_table_index(Table, Index) || Index <- IndexesToDelete ],
      IndexesToAdd = Indexes -- IndexesExisting,
      [ mnesia:add_table_index(Table, Index) || Index <- IndexesToAdd ];
    _AttributesExisting when not TransformFlag -> ok;
    AttributesExisting ->
      IndexesExisting = [ lists:nth(X-1, AttributesExisting)
                          || X <- mnesia:table_info(Table, index) ],
      [ mnesia:del_table_index(Table, Index) || Index <- IndexesExisting ],
      Defaults = maps:get(defaults, Options, #{}),
      TransformFun = transform_function(Table, AttributesExisting, Attributes, Defaults),
      {atomic, ok} = mnesia:transform_table(Table, TransformFun, Attributes),
      [ mnesia:add_table_index(Table, Index) || Index <- Indexes ]
  end.

transform_function(Table, AttributesExisting, Attributes, Defaults) ->
  fun(Row) ->
      list_to_tuple([Table|lists:map(
                             fun(Field) ->
                                 case string:str(AttributesExisting, [Field]) of
                                   0 -> maps:get(Field, Defaults, undefined);
                                   P -> element(P + 1, Row)
                                 end
                             end,
                             Attributes
                            )])
  end.

do_drop(Table) ->
  mnesia:delete_table(Table).

do_clear(Table) ->
  mnesia:clear_table(Table).

do_backup(Options) ->
  BackupFilename = maps:get(file, Options
                           ,application:get_env(backup_file, tivan, ?BACKUP_FILE)),
  TablesToBackup = maps:get(tables, Options
                           ,tables_to_backup()),
  case create_backup_file(BackupFilename) of
    {ok, Fd} ->
      lists:foreach(
        fun(Table) ->
            backup_table(Table, Fd)
        end,
        TablesToBackup
       ),
      close_backup_file(BackupFilename, Fd);
    Error ->
      Error
  end.

create_backup_file(BackupFilename) ->
  TmpFilename = lists:concat([BackupFilename, ".TMP"]),
  file:delete(TmpFilename),
  disk_log:open([{name, make_ref()}, {file, TmpFilename}, {repair, false}, {linkto, self()}]).

tables_to_backup() ->
  mnesia:system_info(tables) -- [schema].

backup_table(Table, Fd) ->
  write_table_def(Table, Fd),
  write_table_recs(Table, Fd).

write_table_def(Table, Fd) ->
  Attributes = mnesia:table_info(Table, attributes),
  Indexes = mnesia:table_info(Table, index),
  StorageType = mnesia:table_info(Table, storage_type),
  Type = mnesia:table_info(Table, type),
  TabDef = {create, Table, Attributes, Indexes, StorageType, Type},
  disk_log:log(Fd, TabDef).

write_table_recs(Table, Fd) ->
  Key = mnesia:dirty_first(Table),
  write_table_recs(Table, Fd, Key).

write_table_recs(_Table, _Fd, '$end_of_table') -> ok;
write_table_recs(Table, Fd, Key) ->
  Recs = mnesia:dirty_read(Table, Key),
  disk_log:log_terms(Fd, Recs),
  NextKey = mnesia:dirty_next(Table, Key),
  write_table_recs(Table, Fd, NextKey).

close_backup_file(BackupFilename, Fd) ->
  disk_log:sync(Fd),
  disk_log:close(Fd),
  file:delete(BackupFilename),
  TmpFilename = lists:concat([BackupFilename, ".TMP"]),
  file:rename(TmpFilename, BackupFilename).

do_restore(Options) ->
  BackupFilename = maps:get(file, Options
                           ,application:get_env(backup_file, tivan, ?BACKUP_FILE)),
  case open_backup_file(BackupFilename) of
    {ok, Fd} ->
      Reply = restore_backup(Fd, Options),
      disk_log:close(Fd),
      Reply;
    Error ->
      Error
  end.

open_backup_file(BackupFilename) ->
  case file:read_file_info(BackupFilename) of
    {error, Reason} ->
      {error, Reason};
    _FileInfo ->
      case disk_log:open([{file, BackupFilename}
                         ,{name, make_ref()}
                         ,{repair, false}
                         ,{mode, read_only}
                         ,{linkto, self()}]) of
        {ok, Fd} ->
          {ok, Fd};
        {repaired, Fd, _, _} ->
          {ok, Fd};
        {error, Reason} ->
          {error, Reason}
      end
  end.

restore_backup(Fd, Options) ->
  restore_backup(Fd, Options, start).

restore_backup(Fd, Options, Start) ->
  case disk_log:chunk(Fd, Start) of
    {error, Reason} ->
      {error, Reason};
    eof ->
      ok;
    {Cont, Chunk} ->
      restore_chunk(Chunk, Options),
      restore_backup(Fd, Options, Cont);
    {Cont, Chunk, _BadBytes} ->
      restore_chunk(Chunk, Options),
      restore_backup(Fd, Options, Cont)
  end.

restore_chunk([], _) -> ok;
restore_chunk([{create, Table, Attributes, Indexes, StorageType, Type}|Chunk], Options) ->
  case lists:member(Table, maps:get(tables, Options, [Table])) of
    true ->
      case maps:get(drop, Options, false) of
        true -> do_drop(Table);
        false -> ok
      end,
      mnesia:create_table(Table, [{attributes, Attributes}
                                  ,{index, Indexes}
                                  ,{StorageType, [node()]}
                                  ,{type, Type}]);
    false -> ignore
  end,
  restore_chunk(Chunk, Options);
restore_chunk([Row|Chunk], Options) ->
  Table = element(1, Row),
  case maps:find(tables, Options) of
    error -> mnesia:dirty_write(Row);
    {ok, Tables} ->
      case lists:member(Table, Tables) of
        true -> mnesia:dirty_write(Row);
        false -> ignore
      end
  end,
  restore_chunk(Chunk, Options).

do_inspect_backup(BackupFilename) ->
  case open_backup_file(BackupFilename) of
    {ok, Fd} ->
      Reply = read_backup(Fd),
      disk_log:close(Fd),
      Reply;
    Error ->
      Error
  end.

read_backup(Fd) ->
  read_backup(Fd, start, #{}).

read_backup(Fd, Start, Info) ->
  case disk_log:chunk(Fd, Start) of
    {error, Reason} ->
      {error, Reason};
    eof ->
      format_info(Info);
    {Cont, Chunk} ->
      InfoU = read_chunk(Chunk, Info),
      read_backup(Fd, Cont, InfoU);
    {Cont, Chunk, _BadBytes} ->
      InfoU = read_chunk(Chunk, Info),
      read_backup(Fd, Cont, InfoU)
  end.

read_chunk([], Info) -> Info;
read_chunk([{create, Table, Attributes, Indexes, StorageType, Type}|Chunk], Info) ->
  read_chunk(Chunk, Info#{Table => {Attributes, Indexes, StorageType, Type, 0}});
read_chunk([Row|Chunk], Info) ->
  Table = element(1, Row),
  TableInfo = case maps:find(Table, Info) of
                error -> ignore;
                {ok, {Attributes, Indexes, StorageType, Type, C}} ->
                  {Attributes, Indexes, StorageType, Type, C+1}
              end,
  read_chunk(Chunk, Info#{Table => TableInfo}).

format_info(Info) ->
  maps:fold(
    fun(Table, {Attributes, Indexes, StorageType, Type, RowCount}, InfoAcc) ->
        TableInfo = #{table => Table
                     ,attributes => Attributes
                     ,indexes => Indexes
                     ,storage_type => StorageType
                     ,type => Type
                     ,row_count => RowCount},
        [TableInfo|InfoAcc]
    end,
    [],
    Info
   ).
