
-include("zaya.hrl").
-include("zaya_copy.hrl").
-include("zaya_atoms.hrl").
-include("zaya_util.hrl").

%---------------------Type modules--------------------------------
-define(m_ets,zaya_ets).
-define(m_ets_leveldb,zaya_ets_leveldb).
-define(m_leveldb,zaya_leveldb).
-define(modules,[?m_ets,?m_ets_leveldb,?m_leveldb]).

%--------------------Schema storage type---------------------------------
-define(schemaModule,
                                        ?m_ets_leveldb
).

%--------------------------------API----------------------------------------
-define(schema,'@schema@').

-define(schemaRef, persistent_term:get(?schema)).

-define(SCHEMA_VALUE(V),

  case length(V) of 0->?undefined; _-> element(2,hd(V)) end

).

-define(schemaRead(K),

  ?SCHEMA_VALUE( ?schemaModule:read(?schemaRef,[K]) )

).
-define(schemaFind(Q),

  ?schemaModule:find(?schemaRef, #{ms => Q})
).

-define(SCHEMA_WRITE(K,V),

  ?schemaModule:write(?schemaRef,[{K,V}])

).

-define(SCHEMA_DELETE(K),

  ?schemaModule:delete(?schemaRef,[K])

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

-define(getSchema,
                       ?schemaFind( [{
                         {'$1','$2'},
                         [],
                         [{{'$1','$2'}}]
                       }])
).


-define(makeSchemaBackup(Dest),

                              ok = file:write_file(Dest,term_to_binary(?getSchema))

).

-define(LOAD_SCHEMA_FROM_BACKUP(Backup),
  begin
                              {ok,_@Schema} = file:read_file(Backup),
                              ok = ?schemaModule:write(?schemaRef, binary_to_term(_@Schema) )
  end
).
%---------------------------------Notifications-----------------------------
-define(schemaSubscribe,
  esubscribe:subscribe(?schema,?readyNodes,self(),infinity)
).

-define(SCHEMA_NOTIFY(A),
  esubscribe:notify(?schema,A)
).

-define(schemaParams,
  #{
    ets => #{},
    leveldb => #{
      dir => ?schemaPath,
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
          verify_checksums => false
          %fill_cache => todo,
          %iterator_refresh =todo
        },
        write => #{
          sync => true
        }
      }
    }
  }
).

-define(SCHEMA_CREATE,

  ?schemaModule:create( ?schemaParams )

).

-define(SCHEMA_OPEN,

  ?SCHEMA_WRITE(?schema, ?schemaModule:open( ?schemaParams ) )

).

-define(SCHEMA_CLOSE,
                              ?schemaModule:close(?schemaRef)

).

-define(SCHEMA_LOAD(SCHEMA),

  ?schemaModule:write(?schemaRef,SCHEMA)

).
-define(SCHEMA_CLEAR,

  ?schemaModule:delete(?schemaRef,[_@K || {_@K,_} <- ?getSchema ])

).

%------------------------------------------------------------------
% public schema search API
%------------------------------------------------------------------
%-------------------------by nodes---------------------------------
-define(allNodes,

  ?schemaFind([{
    {{node,'$1'},'_'},
    [],
    ['$1']
  }])

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

  ?schemaFind([{
      {{node,'$1'},'@up@'},
      [],
      ['$1']
    }])

).
-define(notReadyNodes,

  ?schemaFind([{
      {{node,'$1'},'@down@'},
      [],
      ['$1']
    }])

).

-define(nodeDBs(N),

  ?schemaFind([{
      {{db,'$1','@node@',N,'@params@'},'_'},
      [],
      ['$1']
    }])
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

  ?schemaFind([{
      {{db,'$1','@node@',N,'@params@'},'$2'},
      [],
      [{{'$1','$2'}}]
    }])

).

-define(allNodesDBsParams,

    [ {_@N, ?nodeDBsParams(_@N) } || _@N <- ?allNodes ]

).

%------------------------by DBs--------------------------------
-define(dbModule(DB),

  ?schemaRead({db,DB,'@module@'})

).
-define(dbRef(DB,Node),

  ?schemaRead({db,DB,'@ref@',Node})

).

-define(dbAvailableNodes(DB),

  ?schemaRead({db,DB,'@nodes@'})

).
-define(dbSource(DB),

  case
    ?dbRef(DB,node()) of
    ?undefined->
      case ?dbAvailableNodes(DB) of
        [] -> ?undefined;
        _@Ns-> ?random( _@Ns )
      end;
    _-> node()
  end

).

-define(dbAllNodes(DB),

  ?schemaFind([{
      {{db,DB,'@node@','$1','@params@'},'_'},
      [],
      ['$1']
    }])

).

-define(dbNodeParams(DB,N),

  ?schemaRead({db,DB,'@node@',N,'@params@'})

).


-define(dbNodesParams(DB),

  ?schemaFind([{
      {{db,DB,'@node@','$1','@params@'},'$2'},
      [],
      ['$1','$2']
    }])

).


-define(allDBs,

  ?schemaFind([{
      {'db','$1','@module@'},
      [],
      ['$1']
    }])

).

-define(allDBsNodesParams,

  [ {_@DB, ?dbNodesParams(_@DB)} || _@DB <- ?allDBs ]

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
  case ?dbAvailableNodes(DB) of []-> false; _->true end
).

-define(isDBNotReady(DB),
  case ?dbAvailableNodes(DB) of []-> true; _->false end
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

%=======================================================================================
%             SCHEMA TRANSFORMATION
%=======================================================================================
-define(ADD_DB(DB,M),
  begin
    ?SCHEMA_WRITE({db,DB,'@module@'},M),
    ?SCHEMA_WRITE({db,DB,'@nodes@'}, [] )
  end
).

-define(OPEN_DB(DB,N,Ref),
  begin
    ?SCHEMA_WRITE({db,DB,'@ref@',N},Ref),
    ?SCHEMA_WRITE({db,DB,'@nodes@'}, (?schemaRead({db,DB,'@nodes@'})--[N])++[N] ),
    ?SCHEMA_NOTIFY({'@open_db@',DB,N})
  end
).

-define(CLOSE_DB(DB,N),
  begin
    ?SCHEMA_WRITE({db,DB,'@nodes@'}, ?schemaRead({db,DB,'@nodes@'})--[N] ),
    ?SCHEMA_DELETE({db,DB,'@ref@',N}),
    ?SCHEMA_NOTIFY({'@close_db@',DB,N})
  end
).

-define(ADD_DB_COPY(DB,N,Ps),
  ?SCHEMA_WRITE({db,DB,'@node@',N,'@params@'},Ps)
).

-define(REMOVE_DB_COPY(DB,N),
    ?SCHEMA_DELETE({db,DB,'@node@',N,'@params@'})
).

-define(REMOVE_DB(DB),
  begin
    ?SCHEMA_DELETE({db,DB,'@module@'}),
    ?SCHEMA_DELETE({db,DB,'@nodes@'})
  end
).

-define(NODE_UP(N),
  begin
    ?SCHEMA_WRITE({node,N},'@up@'),
    ?SCHEMA_NOTIFY({'@nodeUp@',N})
  end
).

-define(NODE_DOWN(N),
  begin
    [ ?CLOSE_DB(_@DB,N) || _@DB <- ?nodeDBs(N) ],
    ?SCHEMA_WRITE({node,N},'@down@'),
    ?SCHEMA_NOTIFY({'@nodeDown@',N})
  end
).

-define(REMOVE_NODE(N),
  begin
    [
      begin
        ?CLOSE_DB(_@DB,N),
        ?REMOVE_DB_COPY(_@DB,N)
      end || _@DB <- ?nodeDBs(N)
    ],
    ?SCHEMA_DELETE({node,N})
  end
).



