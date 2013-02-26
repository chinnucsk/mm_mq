%%  Copyright (C) 2012 - Molchanov Maxim

-define(EXCHANGE, <<"optimed">>).
-define(QUEUE_SEP, <<".">>).
-define(KEY_PREFIX, <<"optimed.yourwave_engine">>).

%% Queues
-define(EXCHANGE_KEYS, [
            <<"new">>,
            <<"fetched">>,
            <<"processed">>,
            <<"refused">>
]).

%% Server State
-record(amqp_worker_state, {
        channel,
        connection,
        exchange,
        op_cnt = 0,
        init_time
}).