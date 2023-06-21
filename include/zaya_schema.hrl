
-include("zaya.hrl").
-include("zaya_copy.hrl").
-include("zaya_atoms.hrl").
-include("zaya_util.hrl").

%---------------------Type modules--------------------------------
-define(m_ets,zaya_ets).
-define(m_ets_leveldb,zaya_ets_leveldb).
-define(m_leveldb,zaya_leveldb).
-define(m_rocksdb,zaya_rocksdb).
-define(m_ets_rocksdb,zaya_ets_rocksdb).
-define(m_pterm,zaya_pterm).
-define(m_pterm_leveldb,zaya_pterm_leveldb).

-define(modules,[
  ?m_ets,
  ?m_ets_leveldb,
  ?m_leveldb,
  ?m_rocksdb,
  ?m_ets_rocksdb,
  ?m_pterm,
  ?m_pterm_leveldb
]).

%--------------------Schema storage type---------------------------------
-define(schemaModule,
                                        ?m_pterm_leveldb
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
                              filename:absname(?schemaDir)++"/SCHEMA"
).

-define(schemaExists,
                              filelib:is_file(?schemaPath ++ "/CURRENT")

).

-define(transactionLogPath,
                                ?schemaDir ++ "/LOG"
).

-define(getSchema,
                       ?schemaFind( [{
                         {'$1','$2'},
                         [],
                         [{{'$1','$2'}}]
                       }])
).

%---------------------------------Notifications-----------------------------
-define(schemaSubscribe(PID),
  esubscribe:subscribe(?subscriptions,?schema,PID,?readyNodes)
).

-define(SCHEMA_NOTIFY(A),
  esubscribe:notify(?subscriptions,?schema,A)
).

-define(schemaParams,
  #{
    dir => ?schemaPath,
    pterm => #{},
    leveldb => #{
      eleveldb => #{
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
  persistent_term:put(?schema, ?schemaModule:create( ?schemaParams ))
).

-define(SCHEMA_OPEN,
  persistent_term:put(?schema, ?schemaModule:open( ?schemaParams ))
).

-define(SCHEMA_CLOSE,
  begin
    ?schemaModule:close(?schemaRef),
    persistent_term:erase(?schema)
  end
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

-define(dbRefMod(DB),
  persistent_term:get({db,DB,'@mod_ref@'}, ?undefined)
).

-define(DB_REF_MOD(DB),
  persistent_term:erase({db,DB,'@mod_ref@'})
).
-define(DB_REF_MOD(DB,Ref),
  persistent_term:put({db,DB,'@mod_ref@'}, {Ref, ?dbModule(DB)})
).

-define(dbAvailableNodes(DB),
  ?schemaRead({db,DB,'@nodes@'})
).

-define(DB_AVAILABLE_NODES(DB),
  ?SCHEMA_DELETE({db,DB,'@nodes@'})
).
-define(DB_AVAILABLE_NODES(DB, Ns),
  ?SCHEMA_WRITE({db,DB,'@nodes@'}, Ns )
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
      [{{'$1','$2'}}]
    }])

).


-define(allDBs,

  ?schemaFind([{
      {{db,'$1','@module@'},'_'},
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

-define(isDBAvailable(DB),
  case ?dbAvailableNodes(DB) of []-> false; _->true end
).

-define(isDBNotAvailable(DB),
  case ?dbAvailableNodes(DB) of []-> true; _->false end
).

-define(availableDBs,
  [_@DB || _@DB <- ?allDBs, ?isDBAvailable(_@DB) ]
).

-define(notAvailableDBs,
  [_@DB || _@DB <- ?allDBs, ?isDBNotAvailable(_@DB)]
).

-define(localDBs,
  ?nodeDBs(node())
).

-define(dbMasters(DB),
  case ?schemaRead({db,DB,'@masters@'}) of
    ?undefined -> [];
    _@Ns -> _@Ns
  end
).

-define(dbReadOnly(DB),
  case ?schemaRead({db,DB,'@readonly@'}) of
    ?undefined -> false;
    _@IsReadOnly -> _@IsReadOnly
  end
).

-define(dbReadOnlyVersion(DB),
  ?schemaRead({db,DB,'@readonly_version@'})
).

-define(dbReadOnlyVersion(DB,N),
  ?schemaRead({db,DB,'@readonly_version@',N})
).

%=======================================================================================
%             SCHEMA TRANSFORMATION
%=======================================================================================
-define(ADD_DB(DB,M),
  begin
    ?SCHEMA_WRITE({db,DB,'@module@'},M),
    ?SCHEMA_WRITE({db,DB,'@nodes@'}, [] ),
    ?SCHEMA_NOTIFY({add_db,DB})
  end
).

-define(OPEN_DB(DB,N,Ref),
  begin
    ?SCHEMA_WRITE({db,DB,'@ref@',N},Ref),
    ?DB_AVAILABLE_NODES(DB, (?dbAvailableNodes(DB) -- [N]) ++ [N]),
    if
      N =:= node() ->
        case ?dbReadOnlyVersion(DB) of
          _@ROVer when is_integer(_@ROVer) ->
            ?SCHEMA_WRITE({db,DB,'@readonly_version@',node()},_@ROVer);
          _ ->
            ignore
        end,
        ?DB_REF_MOD(DB, Ref);
      true ->
        ignore
    end,
    ?SCHEMA_NOTIFY({'open_db',DB,N})
  end
).

-define(CLOSE_DB(DB,N),
  begin
    ?DB_AVAILABLE_NODES(DB, ?dbAvailableNodes(DB) -- [N]),
    ?SCHEMA_DELETE({db,DB,'@ref@',N}),
    if
      N =:= node() -> ?DB_REF_MOD(DB);
      true -> ignore
    end,
    ?SCHEMA_NOTIFY({'close_db',DB,N})
  end
).

-define(ADD_DB_COPY(DB,N,Ps),
  begin
    ?SCHEMA_WRITE({db,DB,'@node@',N,'@params@'},Ps),
    ?SCHEMA_NOTIFY({add_db_copy,DB,N})
  end
).

-define(REMOVE_DB_COPY(DB,N),
  begin
    ?SCHEMA_DELETE({db,DB,'@node@',N,'@params@'}),
    ?SCHEMA_DELETE({db,DB,'@readonly_version@',node()}),
    case ?dbMasters(DB) of
      _@Ms when length(_@Ms) >0 ->
        ?SCHEMA_WRITE({db,DB,'@masters@'},_@Ms -- [N]);
      _->
        ignore
    end,
    ?SCHEMA_NOTIFY({remove_db_copy,DB,N})
  end
).

-define(UPDATE_DB_COPY(DB,N,Ps),
  begin
    ?SCHEMA_WRITE({db,DB,'@node@',N,'@params@'},Ps),
    ?SCHEMA_NOTIFY({update_db_copy,DB,N})
  end
).

-define(SET_DB_MASTERS(DB,Ns),
  begin
    if
      is_list(Ns) -> ?SCHEMA_WRITE({db,DB,'@masters@'},Ns);
      true-> ?SCHEMA_DELETE({db,DB,'@masters@'})
    end,
    ?SCHEMA_NOTIFY({db_masters,DB,Ns})
  end
).

-define(SET_DB_READONLY(DB,IsReadOnly),
  begin
    if
      IsReadOnly ->
        __@ROVer = ?dbReadOnlyVersion(DB),
        _@ROVer =
          if
            is_integer(__@ROVer)-> __@ROVer + 1;
            true -> 1
          end,
        ?SCHEMA_WRITE({db,DB,'@readonly_version@'},_@ROVer),
        ?SCHEMA_WRITE({db,DB,'@readonly_version@',node()},_@ROVer),
        ?SCHEMA_WRITE({db,DB,'@readonly@'},true);
      true->
          ?SCHEMA_DELETE({db,DB,'@readonly@'})
    end,
    ?SCHEMA_NOTIFY({db_readonly,DB,IsReadOnly})
  end
).

-define(REMOVE_DB(DB),
  begin
    [ ?REMOVE_DB_COPY(DB,_@N) || _@N <- ?dbAllNodes(DB)],
    ?SCHEMA_DELETE({db,DB,'@module@'}),
    ?SCHEMA_DELETE({db,DB,'@masters@'}),
    ?DB_AVAILABLE_NODES(DB),
    ?SCHEMA_DELETE({db,DB,'@readonly_version@'}),
    ?SCHEMA_DELETE({db,DB,'@readonly@'}),
    ?SCHEMA_NOTIFY({remove_db,DB})
  end
).

-define(NODE_UP(N),
  begin
    ?SCHEMA_WRITE({node,N},'@up@'),
    ?SCHEMA_NOTIFY({'node_up',N})
  end
).

-define(NODE_DOWN(N),
  begin
    [ ?CLOSE_DB(_@DB,N) || _@DB <- ?nodeDBs(N) ],
    ?SCHEMA_WRITE({node,N},'@down@'),
    ?SCHEMA_NOTIFY({'node_down',N})
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
    ?SCHEMA_DELETE({node,N}),
    ?SCHEMA_NOTIFY({'remove_node',N})
  end
).



