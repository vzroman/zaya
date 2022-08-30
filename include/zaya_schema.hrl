
-include("zaya.hrl").
-include("zaya_copy.hrl").
-include("zaya_atoms.hrl").
-include("zaya_util.hrl").

-define(schema,'@schema@').

%---------------------Type modules--------------------------------
-define(m_ets,zaya_ets).
-define(m_ets_leveldb,zaya_ets_leveldb).
-define(m_leveldb,zaya_leveldb).
-define(m_fpdb,zaya_fpdb).
-define(modules,[?m_ets,?m_ets_leveldb,?m_leveldb,?m_fpdb]).

%--------------------Schema storage type---------------------------------
-define(schemaModule,
                                        ?m_ets_leveldb
).

%----------------------Schema server init/terminate API----------------------
-define(schemaDir,
                              ?env(schema_dir, ?DEFAULT_SCHEMA_DIR)
).
-define(schemaPath,
                              filename:absname(?schemaDir)
).

-define(schemaExists,
                              filelib:is_file(?schemaPath ++ "/schema/CURRENT")

).

-define(schemaRef,
                              '@schemaRef@'
).

-define(FIND_RESULT(R),
                          todo
).


-define(getSchema,
                       [ {_@K,_@V} || [_@K,_@V] <- ?schemaFind( [{{'$1','$2'},[],['$1','$2']  }]  ) ]
).


-define(makeSchemaBackup(Dest),
                              ok = file:write_file(Dest,term_to_binary(?getSchema))
).

-define(LOAD_SCHEMA_FROM_BACKUP(Backup),
  begin
                              {ok,_@Schema} = file:read_file(Backup),
                              ok = ?schemaModule:write( binary_to_term(_@Schema) )
  end
).
%---------------------------------Notifications-----------------------------
-define(schemaSubscribe,
  esubscribe:subscribe(?schema,?readyNodes,self(),infinity)
).

-define(SCHEMA_NOTIFY(A),
  esubscribe:notify(?schema,A)
).

%--------------------------------API----------------------------------------
-define(SCHEMA_VALUE(V),
              case length(V) of 0->?undefined; _-> element(2,hd(V)) end
).
-define(schemaRead(K),

                      ?SCHEMA_VALUE(?schemaModule:read(?schema,[K]))

).
-define(schemaFind(Q),

  ?schemaModule:find(?schema, Q)
).

-define(SCHEMA_WRITE(K,V),
                              ?schemaModule:write(?schema,[{K,V}])
  ).
-define(SCHEMA_DELETE(K),
                              ?schemaModule:delete(?schema,[K])
).

-define(SCHEMA_CREATE,

                              ?schemaModule:create(?schema,#{path => ?schemaPath})

).

-define(schemaParams,
  #{
    path => ?schemaPath,
    ets_params =>#{
      named=>false,
      protected=>true,
      type=>ordered_set
    },
    leveld_params=#{
      %compression_algorithm => todo,
      open_options=>#{
        create_if_missing => false,
        error_if_exists => false,
        %write_buffer_size => todo
        %sst_block_size => todo,
        %block_restart_interval = todo,
        %block_size_steps => todo,
        paranoid_checks => true,  % Nice!
        verify_compactions => true,
        compression => false
        %use_bloomfilter => todo,
        %total_memory => todo,
        %total_leveldb_mem => todo,
        %total_leveldb_mem_percent => todo,
        %is_internal_db => todo,
        %limited_developer_mem => todo,
        %eleveldb_threads => TODO pos_integer()
        %fadvise_willneed => TODO boolean()
        %block_cache_threshold => TODO pos_integer()
        %delete_threshold => pos_integer()
        %tiered_slow_level => pos_integer()
        %tiered_fast_prefix => TODO string()
        %tiered_slow_prefix => TODO string()
        %cache_object_warming => TODO
        %expiry_enabled => TODO boolean()
        %expiry_minutes => TODO pos_integer()
        %whole_file_expiry => boolean()
      },
      read => #{
        verify_checksums => true
        %fill_cache => todo,
        %iterator_refresh =todo
      },
      write => #{
        sync => true
      }
    }
  }
).

-define(SCHEMA_OPEN,
  begin

    {ok,_@schemaRef} =
                              ?schemaModule:open(?schema, ?schemaParams ),

                              ?SCHEMA_WRITE(?schemaRef,_@schemaRef)
  end
).

-define(SCHEMA_CLOSE,
                              ?schemaModule:close(?schema)

).

-define(SCHEMA_LOAD(SCHEMA),
  ?schemaModule:write(?schema,SCHEMA)
).
-define(SCHEMA_CLEAR,
  ?schemaModule:delete(?schema,[_@K || {_@K,_} <- ?getSchema ])
).

%------------------------------------------------------------------
% public schema search API
%------------------------------------------------------------------
%-------------------------by nodes---------------------------------
-define(allNodes,

  ?schemaFind(
    [_@N || [_@N] <-
      [{
        {{node,'$1'},'_'},
        [],
        ['$1']
      }
      ]])
).

-define(isNodeReady(N),
  case ?schemaRead({node,N}) of '@up@' ->true; _-> false end
).

-define(isNodeNotReady(N),
  case ?schemaRead({node,N}) of '@up@' -> false; _->true end
).

-define(nodeStatus(N),
  ?schemaRead({node,N})
).

-define(nodesStatus(N),
  [{_@N,?nodeStatus()} || _@N <- ?allNodes ]
).

-define(readyNodes,

  ?schemaFind(
    [_@N || [_@N] <-
      [{
        {{node,'$1'},'@up@'},
        [],
        ['$1']
      }
      ]])

).
-define(notReadyNodes,

  ?schemaFind(
    [_@N || [_@N] <-
      [{
        {{node,'$1'},'@down@'},
        [],
        ['$1']
      }
      ]])

).

-define(nodedbs(N),

  ?schemaFind(
    [_@S || [_@S] <-
      [{
        {{sgm,'$1','@node@',N,'@params@'},'_'},
        [],
        ['$1']
      }
      ]])
).

-define(readyNodesdbs,

  [ {_@N,?nodedbs(_@N)} || _@N <- ?readyNodes]

).
-define(notReadyNodesdbs,

  [ {_@N,?nodedbs(_@N)} || _@N <- ?notReadyNodes]

).

-define(allNodesdbs,

  [ {_@N,?nodedbs(_@N)} || _@N <- ?allNodes]

).

-define(nodedbParams(N,S),

  ?schemaRead({sgm,S,'@node@',N,'@params@'})

).

-define(readyNodesdbParams(S),

  [ {_@N,?nodedbParams(_@N,S)} || _@N<- ?dbReadyNodes(S) ]

).
-define(notReadyNodesdbParams(S),

  [ {_@N,?nodedbParams(_@N,S)} || _@N<- ?dbNotReadyNodes(S) ]

).

-define(nodedbsParams(N),

  ?schemaFind(
    [{_@S,_@SPs} || [_@S,_@SPs] <-
      [{
        {{sgm,'$1','@node@',N,'@params@'},'$2'},
        [],
        ['$1','$2']
      }
      ]])

).

-define(readyNodesdbsParams(N),

  ?schemaFind(
    [{_@S,_@SPs} || [_@S,_@SPs] <-
      [{
        {{sgm,'$1','@node@',N,'@params@'},'$2'},
        [],
        ['$1','$2']
      }
      ]])

).
-define(allNodesdbsParams,

  ?schemaFind(
    [
      {_@N, [{_@S,_@SPs} || {_@S,_@SPs} <- ?nodedbsParams(_@N) ]
      } || _@N <- ?allNodes
    ])

).

%------------------------by dbs--------------------------------
-define(dbModule(S),

  ?schemaRead({'sgm',S,'@module@'})

).
-define(dbRef(S),

  ?schemaRead({sgm,S,'@ref@'})

).


-define(dbAllNodes(S),

  ?schemaFind(
    [_@N || [_@N] <-
      [{
        {{sgm,S,'@node@','$1','@params@'},'_'},
        [],
        ['$1']
      }
      ]])


).
-define(dbNodeParams(S,N),

  ?schemaRead({sgm,S,'@node@',N,'@params@'})

).


-define(dbNodesParams(S),

  ?schemaFind(
    [{_@N,_@NPs} || [_@S,_@NPs] <-
      [{
        {{sgm,S,'@node@','$1','@params@'},'$2'},
        [],
        ['$1','$2']
      }
      ]])

).


-define(alldbs,

  ?schemaFind(
    [_@S || [_@S] <-
      [{
        {'sgm','$1','@module@'},
        [],
        ['$1']
      }
      ]])

).

-define(alldbsNodesParams,

  ?schemaFind(
    [
      {_@S, [{_@N,_@NPs} || {_@N,_@NPs} <- ?dbNodeParams(_@S) ]
      } || _@S <- ?alldbs
    ])

).

-define(dbReadyNodes(S),
  ?dbAllNodes(S) -- ?notReadyNodes
).

-define(dbNotReadyNodes(S),
  ?dbAllNodes(S) -- ?readyNodes
).

-define(dbsReadyNodes(S),
  [{_@S, ?dbReadyNodes(S) } || ?alldbs]
).

-define(dbsNotReadyNodes(S),
  [{_@S, ?dbNotReadyNodes(S) } || ?alldbs]
).

-define(isdbReady(S),
  case ?dbReadyNodes(S) of []-> false; _->true end
).

-define(isdbNotReady(S),
  case ?dbReadyNodes(S) of []-> true; _->false end
).

-define(readydbs,
  [_@S || _@S <- ?alldbs, case ?dbReadyNodes(_@S) of [] -> false; _->true end ]
).

-define(notReadydbs,
  [_@S || _@S <- ?alldbs, case ?dbReadyNodes(_@S)  of [] ->true; _->false end ]
).
-define(localdbs,
  ?nodedbs(node())
).

-define(isLocaldb(S),
  case ?schemaRead( {sgm,S,'@node@','$1','@params@'} ) of ?undefined->false ; _->true end
).
-define( dbSource(S),
  case ?isLocaldb(S) of true->node(); _->?random( ?dbReadyNodes(S) ) end
).

%=======================================================================================
%             SCHEMA TRANSFORMATION
%=======================================================================================
-define(NODE_UP(N),
  begin
    ?SCHEMA_WRITE({node,N},'@up@'),
    ?SCHEMA_NOTIFY({'@nodeUp@',N})
  end
).

-define(NODE_DOWN(N),
  begin
    ?SCHEMA_WRITE({node,N},'@down@'),
    ?SCHEMA_NOTIFY({'@nodeDown@',N})
  end
).

-define(ADD_db(S,M,Ps,Ref),
  begin
    ?SCHEMA_WRITE({'sgm',S,'@module@'},M),
    ?SCHEMA_WRITE({sgm,S,'@ref@'},Ref),
    ?SCHEMA_WRITE({{sgm,S,'@node@',node(),'@params@'},Ps})
  end
).

-define(ADD_db_COPY(S,Ps,Ref),
  begin
    ?SCHEMA_WRITE({sgm,S,'@ref@'},Ref),
    ?SCHEMA_WRITE({{sgm,S,'@node@',node(),'@params@'},Ps})
  end
).

-define(REMOVE_db_COPY(S),
  begin
    ?SCHEMA_DELETE({sgm,S,'@node@',N,'@params@'}),
    ?SCHEMA_DELETE({sgm,S,'@ref@'})
  end
).

-define(REMOVE_db(N,S),
  begin
    ?SCHEMA_DELETE({sgm,S,'@node@',N,'@params@'}),
    ?SCHEMA_DELETE({sgm,S,'@ref@'}),
    ?SCHEMA_DELETE({'sgm',S,'@module@'})
  end
).

-define(REMOVE_NODE(N),
  begin
    [ ?REMOVE_db(N,_@S) || _@S <-?nodedbs(N) ],
    ?SCHEMA_DELETE({node,N})
  end
).


