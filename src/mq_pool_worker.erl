%%  Copyright (C) 2011 - Molchanov Maxim,
%% @copyright 2004-2012 OptimeDev
%% @author Maxim Molchanov <elzor.job@gmail.com>

-module(mq_pool_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../../deps/amqp_client/include/amqp_client.hrl").
-include("include/amqp_exchanges.hrl").

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    {ok, Durable} = config:get(mq_durable_queues),
    AMQParams  = #amqp_params_network{
        username =  proplists:get_value(username, Args),
        password =  proplists:get_value(password, Args),
        host     =  proplists:get_value(host, Args),
        virtual_host =  proplists:get_value(virtual_host, Args),
        channel_max  =  proplists:get_value(channel_max, Args)
    },
    Type = proplists:get_value(type, Args),
    case ampq_connect_and_get_channel(AMQParams, Durable, Type) of
      {ok, {Connection, Channel}} ->
          {ok, #amqp_worker_state{
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

handle_call({send_message, Route, Payload}, _From, #amqp_worker_state{connection=Conn}=State) ->
    {reply, amqp_send_message(Route, Payload, State), State};

handle_call({receive_message, Route}, _From, #amqp_worker_state{connection=Conn}=State) ->
    {reply, amqp_basic_get_message(State#amqp_worker_state.channel, Route), State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #amqp_worker_state{connection=Conn}) ->
    io:format("~nВставить обработчик отключения воркера из пула mq~n"),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
ampq_connect_and_get_channel(Params, Durable, Type) ->
    %% Start a connection to the server
    {ok, Connection} = amqp_connection:start(Params),

    %% Once you have a connection to the server, you can start an AMQP channel

    {ok, Channel} = amqp_connection:open_channel(Connection),
    ExchangeDeclare = #'exchange.declare'{
      exchange = ?EXCHANGE, 
      type     = Type,
      durable  = Durable
    },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
    {ok, {Connection,Channel}}.


amqp_send_message(RoutingKey, Payload, State) ->
    Channel =  State#amqp_worker_state.channel,
    Exchange =  State#amqp_worker_state.exchange,
    BasicPublish = #'basic.publish'{exchange    = Exchange, 
                                    routing_key = RoutingKey},
    NewPayload = utils:payload_encode(Payload),
    {ok, Durable} = config:get(mq_durable_queues),
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


amqp_basic_get_message(Channel, Queue) ->
     case amqp_channel:call(Channel, #'basic.get'{queue = Queue, no_ack = true}) of
        {#'basic.get_ok'{}, Content} ->
          #amqp_msg{payload = Payload} = Content,
          {ok, utils:payload_decode(Payload)};
        Else ->
          {error, Else}
     end.


amqp_setup_consumer(Channel, Q, X, Key, Durable) ->
    QueueDeclare = #'queue.declare'{queue=Q, durable=Durable},
    #'queue.declare_ok'{queue = Q, message_count = MessageCount,consumer_count = ConsumerCount}
                 = amqp_channel:call(Channel, QueueDeclare),
    QueueBind = #'queue.bind'{queue = Q, exchange = X, routing_key = Key},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).


amqp_setup_consumers(Channel, Key, Tot, Durable) ->
    amqp_setup_consumer(Channel, Key, Durable).

amqp_setup_consumer(Channel, QueueKey, Durable) ->
    amqp_setup_consumer(
      Channel,
      QueueKey,
      ?EXCHANGE,
      QueueKey,
      Durable
     ).