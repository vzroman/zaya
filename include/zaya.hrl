
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

-ifndef(TEST).

-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

-else.

-define(LOGERROR(Text),ct:pal("error: "++Text)).
-define(LOGERROR(Text,Params),ct:pal("error: "++Text,Params)).
-define(LOGWARNING(Text),ct:pal("warning: "++Text)).
-define(LOGWARNING(Text,Params),ct:pal("warning: "++Text,Params)).
-define(LOGINFO(Text),ct:pal("info: "++Text)).
-define(LOGINFO(Text,Params),ct:pal("info: "++Text,Params)).
-define(LOGDEBUG(Text),ct:pal("debug: "++Text)).
-define(LOGDEBUG(Text,Params),ct:pal("debug: "++Text,Params)).

-endif.

-endif.



