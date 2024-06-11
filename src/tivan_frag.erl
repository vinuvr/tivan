%%%-------------------------------------------------------------------
%%% @author Chaitanya Chalasani
%%% @copyright (C) 2020, ArkNode.IO
%%% @doc
%%%
%%% @end
%%% Created : 2020-08-21 11:24:37.755973
%%%-------------------------------------------------------------------
-module(tivan_frag).

-behaviour(gen_server).

%% API
-export([create/2
        ,start_link/2
        ,put/2
        ,put/3
        ,get/1
        ,get/2
        ,remove/2
        ,remove/3
        ,update/3
        ,drop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ignore_xref([create/2
        ,start_link/2
        ,put/2
        ,put/3
        ,get/1
        ,get/2
        ,remove/2
        ,remove/3
        ,update/3
        ,drop/1]).

%%%===================================================================
%%% API
%%%===================================================================

create(Table, Options) when is_atom(Table), is_map(Options) ->
  case whereis(Table) of
    undefined ->
      supervisor:start_child(tivan_frag_sofo, [Table, Options]);
    _Pid ->
      gen_server:call(Table, {create, Table, Options})
  end.

start_link(Table, Options) ->
  gen_server:start_link({local, Table}, ?MODULE, [Table, Options], []).

put(Table, ObjectOrObjects) ->
  put(Table, ObjectOrObjects, #{}).

put(Table, ObjectOrObjects, Options) ->
  Frags = persistent_term:get({?MODULE, Table}),
  do_put(Frags, ObjectOrObjects, Options).

get(Table) ->
  get(Table, #{}).

get(Table, Options) ->
  Frags = persistent_term:get({?MODULE, Table}),
  do_get(Frags, Options).

remove(Table, ObjectOrObjects) ->
  remove(Table, ObjectOrObjects, #{}).

remove(Table, ObjectOrObjects, Options) ->
  Frags = persistent_term:get({?MODULE, Table}),
  do_remove(Frags, ObjectOrObjects, Options).

update(Table, Options, Updates) ->
  Frags = persistent_term:get({?MODULE, Table}),
  do_update(Frags, Options, Updates).

drop(Table) ->
  gen_server:cast(Table, {drop, Table}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Table, Options]) ->
  case do_init(Table, Options) of
    {ok, State} ->
      {ok, State};
    {error, Reason} ->
      {stop, Reason}
  end.

handle_call({create, Table, Options}, _From, State) ->
  case do_init(Table, Options) of
    {ok, StateNew} ->
      {reply, ok, StateNew};
    {error, Reason} ->
      {stop, Reason, State}
  end;
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

handle_cast({drop, Table}, State) ->
  tivan:drop(Table),
  {stop, normal, State};
handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_init(Table, #{frag := #{scale := time, slabs := Slabs}} = Options)
  when is_list(Slabs) andalso length(Slabs) > 0 ->
  do_init(Table, time, Slabs, 1, Options, []);
do_init(Table, #{frag := #{scale := size, slabs := Slabs}} = Options)
  when is_list(Slabs) andalso length(Slabs) > 0 ->
  do_init(Table, size, Slabs, 1, Options, []);
do_init(Table, #{frag := #{scale := space, slabs := Slabs}} = Options)
  when is_list(Slabs) andalso length(Slabs) > 0 ->
  do_init(Table, space, Slabs, 1, Options, []);
do_init(_Table, _Options) ->
  {error, bad_frag}.

do_init(Table, Scale, [#{value := Value
                        ,memory := Memory
                        ,persist := Persist}|Slabs], N, Options, State)
  when is_integer(Value), is_boolean(Memory), is_boolean(Persist) ->
  OptionsU = Options#{memory => Memory, persist => Persist},
  TableFrag = list_to_atom(atom_to_list(Table)++"_"++integer_to_list(N)++"_"),
  case tivan:create(TableFrag, OptionsU) of
    ok ->
      do_init(Table, Scale, Slabs, N + 1, Options, [{TableFrag, Scale, Value}|State]);
    Error ->
      Error
  end;
do_init(Table, Scale, [#{memory := Memory, persist := Persist}], N, Options, State)
  when is_boolean(Memory), is_boolean(Persist) ->
  OptionsU = Options#{memory => Memory, persist => Persist},
  do_init(Table, Scale, [], N, OptionsU, State);
do_init(Table, Scale, [], N, Options, State) ->
  TableFrag = list_to_atom(atom_to_list(Table)++"_"++integer_to_list(N)++"_"),
  case tivan:create(TableFrag, Options) of
    ok ->
      StateU = lists:reverse([{TableFrag, Scale, '_'}|State]),
      persistent_term:put({?MODULE, Table}, StateU),
      {ok, StateU};
    Error ->
      Error
  end;
do_init(_Table, _Scale, _Slabs, _N, _Options, _State) ->
  {error, bad_frag}.

do_put([{TableFrag, _Scale, _Value}|_], ObjectOrObjects, Options) ->
  tivan:put(TableFrag, ObjectOrObjects, Options).

do_get(Frags, Options) ->
  lists:flatten(
    [ tivan:get(TableFrag, Options) || {TableFrag, _Scale, _Value} <- Frags ]
   ).

do_remove(Frags, ObjectOrObjects, Options) ->
  [ tivan:remove(TableFrag, ObjectOrObjects, Options) || {TableFrag, _Scale, _Value} <- Frags ].

do_update(Frags, Options, Updates) ->
  lists:flatten(
    [ tivan:update(TableFrag, Options, Updates) || {TableFrag, _Scale, _Value} <- Frags ]
   ).
