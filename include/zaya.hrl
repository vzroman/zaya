
-ifndef(zaya).
-define(zaya,1).

-define(DEFAULT_SCHEMA_DIR,"./DB").

-define(DEFAULT_START_TIMEOUT, 60000). % 10 min.
-define(DEFAULT_STOP_TIMEOUT,600000). % 10 min.

-define(WAIT_SCHEMA_TIMEOUT,24*3600*1000). % 1 day
-define(ATTACH_TIMEOUT,600000). %10 min.

-define(DEFAULT_MAX_RESTARTS,10).
-define(DEFAULT_MAX_PERIOD,1000).

-define(DEFAULT_DB_RECOVERY_ATTEMPTS,5).

%%-------------------------------------------------------------------------------
%% LOGGING
%%-------------------------------------------------------------------------------

-ifndef(TEST).

-define(MFA_METADATA, #{
  mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY},
  line => ?LINE
}).

-define(LOGERROR(Text),          logger:error(Text, [], ?MFA_METADATA)).
-define(LOGERROR(Text,Params),   logger:error(Text, Params, ?MFA_METADATA)).
-define(LOGWARNING(Text),        logger:warning(Text, [], ?MFA_METADATA)).
-define(LOGWARNING(Text,Params), logger:warning(Text, Params, ?MFA_METADATA)).
-define(LOGINFO(Text),           logger:info(Text, [], ?MFA_METADATA)).
-define(LOGINFO(Text,Params),    logger:info(Text, Params, ?MFA_METADATA)).
-define(LOGDEBUG(Text),          logger:debug(Text, [], ?MFA_METADATA)).
-define(LOGDEBUG(Text,Params),   logger:debug(Text, Params, ?MFA_METADATA)).

-else.

-define(LOGERROR(Text),           ct:pal("error: " ++ Text)).
-define(LOGERROR(Text, Params),   ct:pal("error: " ++ Text, Params)).
-define(LOGWARNING(Text),         ct:pal("warning: " ++ Text)).
-define(LOGWARNING(Text, Params), ct:pal("warning: " ++ Text, Params)).
-define(LOGINFO(Text),            ct:pal("info: " ++ Text)).
-define(LOGINFO(Text, Params),    ct:pal("info: " ++ Text, Params)).
-define(LOGDEBUG(Text),           ct:pal("debug: " ++ Text)).
-define(LOGDEBUG(Text, Params),   ct:pal("debug: " ++ Text, Params)).

-endif.

-endif.



