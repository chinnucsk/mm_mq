%%  Copyright (C) 2012 - Molchanov Maxim,

-author('author Maxim Molchanov <elzor.job@gmail.com>').


%% -----------------------------------------------------------------------------
%%  Common tools
%% -----------------------------------------------------------------------------
-ifdef(DEBUG).
-define(DEB(Str, Args), io:format(Str,Args)).
-define(SHOW_MSG,1).
-else.
-define(DEB(Str, Args), ok).
-define(SHOW_MSG,0).
-endif.
-define(VALUE(Call),io:format("~p = ~p~n",[??Call,Call])).

-define(WEB_TIMEOUT, 10000).