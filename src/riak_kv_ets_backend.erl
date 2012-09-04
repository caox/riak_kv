%% -------------------------------------------------------------------
%%
%% riak_kv_bitcask_backend: Bitcask Driver for Riak
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_kv_ets_backend).

-behavior(riak_kv_backend).
-author('Xu Cao <cao.xu@rytong.com>').
%%
%% Include files
%%

-define(API_VERSION, 1).
-define(CAPABILITIES, []).

-record(state, {tab :: integer(),
                opts :: [{atom(), term()}],
                partition :: integer()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%%
%% Exported Functions
%%
%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

%%
%% API Functions
%%

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the ets backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    error_logger:info_msg("The config for ets backend  :~p~n", [Config]),
    case catch (ets:new(partition, [public, {write_concurrency, true}])) of
        {'EXIT', Err} ->
            {error, Err};
        Tab ->
            {ok, #state{tab = Tab,
                        opts = Config,
                        partition = Partition}}
    end.

%% @doc Stop the ets backend
-spec stop(state()) -> ok.
stop(#state{tab=Tab}) ->
    catch(ets:delete(Tab)),
    ok.

%% @doc Retrieve an object from the ets backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {error, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{tab=Tab}=State) ->
    case ets:lookup(Tab, {Bucket, Key}) of
        [{_, Value}|_] ->
            {ok, Value, State};
        _  ->
            {error, not_found, State}
    end.

%% @doc Insert an object into the ets backend.
%% NOTE: The ets backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, _IndexSpecs, Val, #state{tab=Tab}=State) ->
    case ets:insert(Tab, {{Bucket, PrimaryKey}, Val}) of
        true ->
            {ok, State};
        Err ->
            {error, Err, State}
    end.

%% @doc Delete an object from the ets backend
%% NOTE: The ets backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{tab=Tab}=State) ->
    case ets:delete(Tab, {Bucket, Key}) of
        true ->
            {ok, State};
        Reason ->
            {error, Reason, State}
    end.


%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_buckets(_FoldBucketsFun, _Acc, _Opts, #state{}) ->
    {error, not_supported}.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()} | {error, term()}.
fold_keys(_FoldKeysFun, _Acc, _Opts, #state{}) ->
    {error, not_supported}.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_objects(_FoldObjectsFun, _Acc, _Opts, #state{}) ->
    {error, not_supported}.

%% @doc Delete all objects from this ets backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{tab=Tab}=State) ->
    ets:delete_all_objects(Tab),
    {ok, State}.

%% @doc Returns true if this ets backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{tab=Tab}) ->
    ets:info(Tab, size) =:= 0.

%% @doc Get the status information for this bitcask backend
-spec status(state()) -> [{atom(), term()}].
status(#state{tab=Tab}) ->
    ets:info(Tab).


%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.

%% Ignore callbacks for other backends so multi backend works
callback(_Ref, _Msg, State) ->
    {ok, State}.


%%
%% Local Functions
%%

