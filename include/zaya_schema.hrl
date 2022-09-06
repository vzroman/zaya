
-include("zaya.hrl").
-include("zaya_copy.hrl").
-include("zaya_atoms.hrl").
-include("zaya_util.hrl").

-define(schema,'@schema@').

%---------------------Type modules--------------------------------
-define(m_ets,zaya_ets).
-define(m_ets_leveldb,zaya_ets_leveldb).
-define(m_leveldb,zaya_leveldb).
-define(m_ecomet,zaya_ecomet).
-define(modules,[?m_ets,?m_ets_leveldb,?m_leveldb,?m_ecomet]).

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

                      ?SCHEMA_VALUE( ?schemaModule:read(?schema,[K]) )

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
    ets_params => #{
      named=>false,
      protected=>true,
      type=>ordered_set
    },
    leveld_params => #{
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

-define(nodeDBs(N),

  ?schemaFind(
    [_@DB || [_@DB] <-
      [{
        {{db,'$1','@node@',N,'@params@'},'_'},
        [],
        ['$1']
      }
      ]])
).

-define(readyNodesDBs,

  [ {_@N,?nodeDBs(_@N)} || _@N <- ?readyNodes]

).
-define(notReadyNodesDBs,

  [ {_@N,?nodeDBs(_@N)} || _@N <- ?notReadyNodes]

).

-define(allNodesDBs,

  [ {_@N,?nodeDBs(_@N)} || _@N <- ?allNodes]

).

-define(nodeDBParams(N,DB),

  ?schemaRead({db,DB,'@node@',N,'@params@'})

).

-define(readyNodesDBParams(DB),

  [ {_@N,?nodeDBParams(_@N,DB)} || _@N<- ?dbReadyNodes(DB) ]

).
-define(notReadyNodesDBParams(DB),

  [ {_@N,?nodeDBParams(_@N,DB)} || _@N<- ?dbNotReadyNodes(DB) ]

).

-define(nodeDBsParams(N),

  ?schemaFind(
    [{_@DB,_@Ps} || [_@DB,_@Ps] <-
      [{
        {{db,'$1','@node@',N,'@params@'},'$2'},
        [],
        ['$1','$2']
      }
      ]])

).

-define(allNodesDBsParams,

  ?schemaFind(
    [
      {_@N, ?nodeDBsParams(_@N) } || _@N <- ?allNodes
    ])

).

%------------------------by DBs--------------------------------
-define(dbModule(DB),

  ?schemaRead({db,DB,'@module@'})

).
-define(dbRef(DB),

  ?schemaRead({db,DB,'@ref@'})

).


-define(dbAllNodes(DB),

  ?schemaFind(
    [_@N || [_@N] <-
      [{
        {{db,DB,'@node@','$1','@params@'},'_'},
        [],
        ['$1']
      }
      ]])


).
-define(dbNodeParams(DB,N),

  ?schemaRead({db,DB,'@node@',N,'@params@'})

).


-define(dbNodesParams(DB),

  ?schemaFind(
    [{_@N,_@Ps} || [_@S,_@Ps] <-
      [{
        {{db,DB,'@node@','$1','@params@'},'$2'},
        [],
        ['$1','$2']
      }
      ]])

).


-define(allDBs,

  ?schemaFind(
    [_@DB || [_@DB] <-
      [{
        {'db','$1','@module@'},
        [],
        ['$1']
      }
      ]])

).

-define(allDBsNodesParams,

  ?schemaFind(
    [
      {_@DB, ?dbNodesParams(_@DB)} || _@DB <- ?allDBs
    ])

).

-define(dbReadyNodes(DB),
  ?dbAllNodes(DB) -- ?notReadyNodes
).

-define(dbNotReadyNodes(DB),
  ?dbAllNodes(DB) -- ?readyNodes
).

-define(dbsReadyNodes,
  [{_@DB, ?dbReadyNodes(_@DB) } || _@DB <- ?allDBs]
).

-define(dbsNotReadyNodes,
  [{_@DB, ?dbNotReadyNodes(_@DB) } || _@DB <- ?allDBs]
).

-define(isDBReady(DB),
  case ?dbReadyNodes(S) of []-> false; _->true end
).

-define(isDBNotReady(DB),
  case ?dbReadyNodes(DB) of []-> true; _->false end
).

-define(readyDBs,
  [_@DB || _@DB <- ?allDBs, ?isDBReady(_@DB) ]
).

-define(notReadyDBs,
  [_@DB || _@DB <- ?allDBs, ?isDBNotReady(_@DB)]
).
-define(localDBs,
  ?nodeDBs(node())
).

-define(isLocalDB(DB),
  case ?schemaRead( {db,DB,'@node@',node(),'@params@'} ) of ?undefined->false ; _->true end
).
-define(dbSource(DB),
  case ?isLocalDB(DB) of true->node(); _->?random( ?dbReadyNodes(DB) ) end
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

-define(ADD_DB(DB,M),
  ?SCHEMA_WRITE({db,DB,'@module@'},M)
).

-define(OPEN_DB(DB,Ref),
    ?SCHEMA_WRITE({db,DB,'@ref@'},Ref)
).

-define(CLOSE_DB(DB),
  ?SCHEMA_DELETE({db,DB,'@ref@'})
).

-define(ADD_DB_COPY(DB,N,Ps),
  ?SCHEMA_WRITE({db,DB,'@node@',N,'@params@'},Ps)
).

-define(REMOVE_DB_COPY(DB,N),
    ?SCHEMA_DELETE({db,DB,'@node@',N,'@params@'})
).

-define(REMOVE_DB(DB),
  begin
    [?REMOVE_DB_COPY(DB,_@N) || _@N <- ?dbAllNodes(DB) ],
    ?CLOSE_DB(DB),
    ?SCHEMA_DELETE({db,DB,'@module@'})
  end
).

-define(REMOVE_NODE(N),
  begin
    [ ?REMOVE_DB_COPY(_@DB,N) || _@DB <-?nodeDBs(N) ],
    ?SCHEMA_DELETE({node,N})
  end
).

% TODO:


