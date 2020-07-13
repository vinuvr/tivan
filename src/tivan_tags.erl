%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-06-20 21:35:56.736576
%%%-------------------------------------------------------------------
-module(tivan_tags).

%% API
-export([create/1
        ,tag/3
        ,untag/3
        ,tags/2
        ,entities/2]).

%%%===================================================================
%%% API
%%%===================================================================

create(Name) ->
  tivan:create(Name, #{columns => [entity, {tag}], type => bag}).

tag(Name, Entity, TagUnknownCase) ->
  Tag = string:uppercase(TagUnknownCase),
  tivan:put(Name, #{entity => Entity, tag => Tag}).

untag(Name, Entity, TagUnknownCase) ->
  Tag = string:uppercase(TagUnknownCase),
  tivan:remove(Name, #{entity => Entity, tag => Tag}).

tags(Name, Entity) ->
  TagMaps = tivan:get(Name, #{match => #{entity => Entity}, select => [tag]}),
  [ T || #{tag := T} <- TagMaps ].

entities(Name, Tags) when is_list(Tags) ->
  case lists:filtermap(
         fun(TagUnknownCase) ->
             Tag = string:uppercase(TagUnknownCase),
             case tivan:get(Name, #{match => #{tag => Tag}
                                   ,select => [entity]}) of
               [] -> false;
               Entities -> {true, [ E || #{entity := E} <- Entities ]}
             end
         end,
         Tags
        ) of
    [] -> undefined;
    [EntityGroup|EntityGroups] ->
      lists:filter(
        fun(Entity) ->
            lists:all(
              fun(OtherEntityGroup) ->
                  lists:member(Entity, OtherEntityGroup)
              end,
              EntityGroups
             )
        end,
        EntityGroup
       )
  end;
entities(Name, Tag) -> entities(Name, [Tag]).

