%%  Copyright (C) 2011 - Molchanov Maxim,
%% @copyright 2004-2012 OptimeDev
%% @author Maxim Molchanov <elzor.job@gmail.com>

-module(mq_pool).

-behaviour(supervisor).

%% API
-export([send_msg/3, receive_msg/2, get_state/1, setup_consumer/5, subscribe_process/3]).
-export([start_link/0]).
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, Pools} = config:get(mq_pool),
    PoolSpecs = lists:map(
                    fun({PoolName, SizeArgs, WorkerArgs}) ->
                        Args = [{name, {local, PoolName}},
                                {worker_module, mq_pool_worker}]
                                ++ SizeArgs,
                        poolboy:child_spec(PoolName, Args, WorkerArgs)
                    end,
                    Pools
                ),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.

send_msg(PoolName, Route, Payload) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {send_message,  Route, Payload})
    end).

receive_msg(PoolName, Route) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {receive_message, Route})
    end).

get_state(PoolName)->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {state})
    end).    

setup_consumer(PoolName, Channel, QueueKey, TotQueues, Durable)->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {setup_consumer, Channel, QueueKey, TotQueues, Durable})
    end).        

subscribe_process(PoolName, Route, Pid)->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:cast(Worker, {subscribe_process, Route, Pid})
    end).    