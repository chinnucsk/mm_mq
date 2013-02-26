%%  Copyright (C) 2012 - Molchanov Maxim,
%%  Site: http://optimedev.com
%% @copyright 2004-2012 OptimeDev
%% @author Maxim Molchanov <elzor.job@gmail.com>
%%  Stable module: 20.05.2012  by Elzor
%%  Last edition:  24.05.2012  by Elzor

-module(mq).

-behaviour(gen_server).

-include("../../deps/amqp_client/include/amqp_client.hrl").
-include("include/base.hrl").

-export([start/0, start_link/0, stop/0, init/1, state/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% API
-export([
    amqp_setup_consumers/4,
    send_new/1,
    send_processed/1,
    send_refused/1,
    send_undef/1,
    receive_new/0,
    receive_processed/0,
    receive_refused/0,
    receive_undef/0
    ]).

-define(EXCHANGE, <<"optimed">>).
-define(QUEUE_SEP, <<".">>).
-define(KEY_PREFIX, <<"optimed.defapp">>).
-define(EXCHANGE_KEYS, [
            <<"new">>,
            <<"processed">>,
            <<"refused">>,
            <<"undef">>,
            <<"new_free">>,
            <<"processed_free">>,
            <<"refused_free">>,
            <<"undef_free">>
           ]).

%% Server State
-record(state, {
        channel,
        connection,
        exchange,
        op_cnt = 0,
        init_time
}).
-record(key_struct, {name, payload}).

%% Public API
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start() ->
  gen_server:start({local, ?MODULE}, ?MODULE, [], []).

stop(Module) ->
  gen_server:call(Module, stop, infinity).

stop() ->
  stop(?MODULE).

state(Module) ->
  gen_server:call(Module, state, infinity).

state() ->
  state(?MODULE).
 
statistics() ->
    gen_server:call(?MODULE, {statistics}, infinity).

ping()->
    gen_server:call(?MODULE, {ping}, infinity).  

reconnect()->
  gen_server:cast(?MODULE, {reconnect}).

send_new(Payload) ->
    gen_server:cast(?MODULE, {send, def, <<"new">>, Payload}).

send_processed(Payload) ->
    gen_server:cast(?MODULE, {send, def, <<"processed">>, Payload}).

send_refused(Payload) ->
    gen_server:cast(?MODULE, {send, def, <<"refused">>, Payload}).

send_undef(Payload) ->
    gen_server:cast(?MODULE, {send, def, <<"undef">>, Payload}).

receive_new() ->
    gen_server:call(?MODULE, {receive_msg, <<"new">>, def}, infinity).

receive_processed() ->
    gen_server:call(?MODULE, {receive_msg, <<"processed">>, def}, infinity).

receive_refused() ->
    gen_server:call(?MODULE, {receive_msg, <<"refused">>, def}, infinity).

receive_undef() ->
    gen_server:call(?MODULE, {receive_msg, <<"undef">>, def}, infinity).

terminate(_Reason, _State) ->
  ok.

%% API


%% Server implementation, a.k.a.: callbacks

init([]) ->
  {ok, Durable} = application:get_env(optimed, mq_durable_queues),
  {ok, Params} = config:get(mq_connection_params),
  AMQParams = #amqp_params_network{
      username =  proplists:get_value(username, Params),
      password =  proplists:get_value(password, Params),
      host =  proplists:get_value(host, Params),
      virtual_host =  proplists:get_value(virtual_host, Params),
      channel_max =  proplists:get_value(channel_max, Params)
  },
  case ampq_connect_and_get_channel(AMQParams, Durable) of
  {ok, {Connection, Channel}} ->
      {ok, #state{
         channel = Channel,
         connection = Connection,
         exchange = ?EXCHANGE,
         op_cnt = 1,
         init_time = utils:unix_timestamp()
        }
      };
  _Else ->
      {error, amqp_cannot_connect_or_get_channel}
      %% TODO stopping the application or wait 60 sec and run again init
  end.


%
%   CALL
%

handle_call({receive_msg, Key, Depth}, _From, State) ->
    Reply = amqp_receive(Key, Depth, State),
    NewState = State#state{op_cnt=State#state.op_cnt+1},
    {reply, Reply, NewState};

handle_call(stop, _From, State) ->
  say("stopping by ~p, state was ~p.", [_From, State]),
  {stop, normal, stopped, State};

handle_call(state, _From, State) ->
  {reply, State, State};

handle_call({ping}, _From, State) ->
  {reply, pong, State};
 


handle_call(_Request, _From, State) ->
  say("call ~p, ~p, ~p.", [_Request, _From, State]),
  {reply, ok, State}.

%
%   CAST
%

handle_cast({send, Tracker, Key, Payload}, State) ->
    amqp_send(Tracker, Key, Payload, State),
    NewState = State#state{op_cnt=State#state.op_cnt+1},
    {noreply, NewState};

handle_cast(_Msg, State) ->
  say("cast ~p, ~p.", [_Msg, State]),
  {noreply, State}.

handle_info(_Info, State) ->
  say("info ~p, ~p.", [_Info, State]),
  {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
  say("code_change ~p, ~p, ~p", [_OldVsn, State, _Extra]),
  {ok, State}.

%% Some helper methods.

say(Format) ->
  say(Format, []).
say(Format, Data) ->
  io:format("~p:~p: ~s~n", [?MODULE, self(), io_lib:format(Format, Data)]).


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
ampq_connect_and_get_channel(Params, Durable) ->
    %% Start a connection to the server
    {ok, Connection} = amqp_connection:start(Params),

    %% Once you have a connection to the server, you can start an AMQP channel
    %% TODO : verify 

    {ok, Channel} = amqp_connection:open_channel(Connection),
    ExchangeDeclare = #'exchange.declare'{
      exchange = ?EXCHANGE, 
      type = <<"topic">>,
      durable = Durable
     },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
    {ok, {Connection,Channel}}.


amqp_setup_consumers(Channel, Key, Tot, Durable) ->
    amqp_setup_consumer(
            Channel,
            Key,
            Durable
    ).

amqp_setup_consumer(Channel, QueueKey, Durable) ->
    amqp_setup_consumer(
      Channel,
      QueueKey,
      ?EXCHANGE,
      QueueKey,
      Durable
     ).

amqp_setup_consumer(Channel, Q, X, Key, Durable) ->
    QueueDeclare = #'queue.declare'{queue=Q, durable=Durable},
    #'queue.declare_ok'{queue = Q, message_count = MessageCount,consumer_count = ConsumerCount}
                 = amqp_channel:call(Channel, QueueDeclare),
    QueueBind = #'queue.bind'{queue = Q, exchange = X, routing_key = Key},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).

get_queue_name(Key)->
  get_queue_name(Key, 0).
get_queue_name(Key, Depth) ->
    <<?KEY_PREFIX/binary,?QUEUE_SEP/binary,Key/binary>>.

amqp_basic_get_message(Channel, Queue) ->
     case amqp_channel:call(Channel, #'basic.get'{queue = Queue, no_ack = true}) of
        {#'basic.get_ok'{}, Content} ->
          #amqp_msg{payload = Payload} = Content,
          {ok, utils:payload_decode(Payload)};
        Else ->
          {error, Else}
     end.

amqp_receive(Key, Tracker, State) ->
    Channel =  State#state.channel,
    Queue = get_queue_name(Key, Tracker),
    amqp_basic_get_message(Channel, Queue).

cnt_all_messages(Tracker, Key, State) ->
    {ok, Durable} = application:get_env(etorrentino, mq_durable_queues),
    Channel =  State#state.channel,
    Queue = get_queue_name(Key, Tracker),
    QueueDeclare = #'queue.declare'{queue=Queue, durable=Durable},
    #'queue.declare_ok'{queue = Queue, message_count = MessageCount,consumer_count = ConsumerCount}
                         = amqp_channel:call(Channel, QueueDeclare),
    {ok, MessageCount, ConsumerCount}.


amqp_send(Tracker, Key, Payload, State) ->    
    RoutingKey = get_queue_name(Key, Tracker),
    amqp_send_message(RoutingKey, Payload, State).

amqp_send_message(RoutingKey, Payload, State) ->
    Channel =  State#state.channel,
    Exchange =  State#state.exchange,
    BasicPublish = #'basic.publish'{exchange = Exchange, 
            routing_key = RoutingKey},
    NewPayload = utils:payload_encode(Payload),
    {ok, Durable} = application:get_env(optimed, mq_durable_queues),
    case Durable of
      true ->
          Msg = #amqp_msg{
            payload = NewPayload,
            props = #'P_basic'{delivery_mode=2}
           };
      false ->
          Msg = #amqp_msg{
            payload = NewPayload
           }
    end,
    case Result = amqp_channel:cast(Channel, BasicPublish, _MsgPayload = Msg) of
      ok ->
        case config:get(debug) of
            {ok, true}->
              error_logger:info_report({?MODULE, ?LINE, {send_url, RoutingKey, Payload}});
            _Else->
              pass
        end;
      else ->
        error_logger:error_report({?MODULE, ?LINE, {cannot_send_url, RoutingKey, Payload}})
    end,
    Result.