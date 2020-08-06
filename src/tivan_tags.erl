%%%-------------------------------------------------------------------
%%% @author Chaitanya Chalasani
%%% @copyright (C) 2019, ArkNode.IO
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
        ,entities/2
        ,entities/3]).

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
  entities(Name, Tags, inclusive);
entities(Name, TagUnknownCase) ->
  Tag = string:uppercase(TagUnknownCase),
  Entities = tivan:get(Name, #{match => #{tag => Tag}, select => [entity]}),
  [ E || #{entity := E} <- Entities ].

entities(Name, Tags, inclusive) ->
  lists:usort(
    lists:foldl(
      fun(Tag, Entities) ->
          Entities ++ entities(Name, Tag)
      end,
      [],
      Tags
     )
   );
entities(Name, Tags, exclusive) ->
  case lists:filtermap(
         fun(Tag) ->
             case entities(Name, Tag) of
               [] -> false;
               Entities -> {true, Entities}
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
  end.

