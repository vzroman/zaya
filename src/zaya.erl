
-module(zaya).

%%=================================================================
%%	Service API
%%=================================================================
-export([
%%  add_node/1,remove_node/1,
%%  get_nodes/0,
%%  get_ready_nodes/0,
%%  get_active_nodes/0,
%%  get_storages/0,
%%  get_storage_type/1,
%%  get_storage_root/1,
%%  is_local_storage/1,
%%  get_storage_efficiency/1,
%%  rebalance_storage/1,
%%  get_dbs/0,get_dbs/1,
%%  get_node_dbs/1,
%%  get_local_dbs/0,
%%  get_db_info/1,
%%  get_db_params/1,
%%  verify_hash/0, verify_hash/1,
%%  get_db_size/1,
%%  add_storage/2,add_storage/3,
%%  storage_limits/1, storage_limits/2,
%%  default_limits/0, default_limits/1,
%%  backend_env/0,backend_env/1,
%%  ADD_DB_copy/2, REMOVE_DB_COPY/2,
%%  remove_storage/1,
%%  stop/0
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
%%  transaction/1, sync_transaction/1,
%%  read/2, read/3, dirty_read/2,
%%  write/3, write/4, dirty_write/3,
%%  delete/2, delete/3, dirty_delete/2,
%%  first/1, dirty_first/1, last/1, dirty_last/1,
%%  next/2, dirty_next/2, prev/2, dirty_prev/2,
%%  dirty_range_select/3, dirty_range_select/4
]).

%%-type first() :: '$first'.
%%-type last() :: '$last'.
%%-type not_available() :: [] | to_end() | not_available.
%%-type key() :: from_start() | to_end() | any().
%%-type value() :: any().
%%-type rec() :: tuple(key(),value()).
%%-type records() :: list(rec()).
%%-type keys() :: list(key()).
%%-type db() :: atom().
%%-type storage() :: atom().
%%-type storage_type() :: ets | ets_eleveldb | eleveldb.
%%-type is_shared() :: boolean().
%%-type is_global() :: boolean().
%%-type timeout() :: integer() | infinity.
%%-type storage_params() :: map().
%%-type db_params() :: map().
%%-type db_info() :: map().


%%%%---------------------------------------------------------------
%%%%	SERVICE API
%%%%---------------------------------------------------------------
%%
%%%-----------------------------------------------------------------
%%%% @doc Add a new node to the schema.
%%%% Returns true when a new node is added or false in other case
%%%% @end
%%%-----------------------------------------------------------------
%%-spec add_node(Node :: node()) -> true | false.
%%add_node(Node)->
%%  zaya_node_srv:add( Node ).
%%
%%
%%%-----------------------------------------------------------------
%%%% @doc  Remove a node from the schema.
%%% Returns {atomic, ok} when node is removed or {aborted, Reason}
%%% in negative case
%%%% @end
%%%-----------------------------------------------------------------
%%-spec remove_node(Node :: node()) -> ok | {error, any()}.
%%remove_node(Node)->
%%  zaya_node_srv:remove( Node ).
%%
%%%-----------------------------------------------------------------
%%%% @doc	Get list of all zaya nodes
%%% Returns
%%% [node1,node2 ..]
%%% where the type of a name of a node is an atom
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_nodes() -> ListOfNode :: list(node()).
%%get_nodes()->
%%  zaya_node_srv:get_nodes().
%%
%%%-----------------------------------------------------------------
%%%% @doc	Get list of ready zaya nodes
%%% Returns
%%% [node1,node2 ..]
%%% where the type of a name of a node is an atom
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_ready_nodes() -> ListOfNode :: list(node()).
%%get_ready_nodes()->
%%  zaya_node_srv:get_ready_nodes().
%%
%%%-----------------------------------------------------------------
%%%% @doc	Get list of all zaya storages.
%%% Returns:
%%% [storage1,storage2 ..]
%%% where the type of name of storage is the atom
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_storages() -> ListOfStorage :: list(storage()).
%%get_storages()->
%%  zaya_storage:get_storages().
%%
%%%-----------------------------------------------------------------
%%%% @doc	Get storage type.
%%%% Returns: ram | ramdisc | disc.
%%%% Each type defines where is the storage was added.
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_storage_type(Storage :: storage()) -> {ok,storage_type()} | {error,not_exists}.
%%get_storage_type(Storage) ->
%%  zaya_storage:get_type(Storage).
%%
%%%-----------------------------------------------------------------
%%%% @doc	Get storage root db.
%%%% Returns: the name of the current root db of the storage.
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_storage_root(Storage :: storage()) -> db() | {error,not_exists}.
%%get_storage_root(Storage) ->
%%  zaya_storage:root_db(Storage).
%%%-----------------------------------------------------------------
%%%% @doc Check if the storage has local only content
%%%% @end
%%%-----------------------------------------------------------------
%%-spec is_local_storage(Storage :: storage()) -> boolean().
%%is_local_storage(Storage) ->
%%  zaya_storage:is_local(Storage).
%%
%%%-----------------------------------------------------------------
%%%% @doc Get list of all zaya dbs.
%%% Returns:
%%% [zaya_storage1_1,zaya_storage1_2,zaya_storage2_1 ..],
%%% where the element of list is the name of dbs and has type
%%% of atom
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_dbs() -> allDBs :: list(db()).
%%get_dbs()->
%%  zaya_storage:get_dbs().
%%
%%%-----------------------------------------------------------------
%%%% @doc Get list of zaya dbs for the Storage.
%%% Returns:
%%% [zaya_storage1_1,zaya_storage1_2,zaya_storage1_3 ..]
%%% where the element of list is the name of dbs of storage storage1
%%% and has type of atom
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_dbs(Storage :: storage()) -> Storagedbs :: list(db()).
%%get_dbs(Storage)->
%%  zaya_storage:get_dbs(Storage).
%%
%%%-----------------------------------------------------------------
%%%% @doc Get list of zaya dbs for the Node.
%%% Returns:
%%% [zaya_storage1_1,zaya_storage1_2,zaya_storage1_3 ..]
%%% where the element of list is the name of dbs of storage storage1
%%% and has type of atom
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_node_dbs(Node :: node()) -> nodeDBs :: list(db()).
%%get_node_dbs(Node)->
%%  zaya_storage:get_dbs(Node).
%%
%%%-----------------------------------------------------------------
%%%% @doc Get list of zaya dbs that has local copies.
%%% Returns:
%%% [zaya_storage1_1,zaya_storage1_2,zaya_storage1_3 ..]
%%% where the element of list is the name of dbs
%%% and has type of atom
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_local_dbs() -> nodeDBs :: list(db()).
%%get_local_dbs()->
%%  zaya_db:local_dbs().
%%
%%%-----------------------------------------------------------------
%%%% @doc  Get db info.
%%% Returns map:
%%% #{
%%%   type => Type,      :: disc | ram | ramdisc
%%%   local => Local,    :: true | false
%%%   nodes => Nodes     :: list of atom() [node(),..]
%%%   }
%%% or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_db_info(db :: db(),Node::node()) -> {ok,db_info()} | {error,any()}.
%%get_db_info(db,Node) ->
%%  zaya_db:get_info(db,Node).
%%
%%%-----------------------------------------------------------------
%%%% @doc  Get db params.
%%% Returns map:
%%% #{
%%%   storage => NameOfStoragedbBelongsTo,
%%%   level => LevelOfdbInStorage,
%%%   key => TheFirstKeyIndb,
%%%   version => VersionOfdb,
%%%   copies => #{
%%%     <node1> => #{ Params as hash etc },
%%%     <node2> => #{ Params as hash etc },
%%%     ...
%%%   }
%%% }
%%% or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_db_params(db :: db()) -> dbParams :: {ok,db_params()} | {error,not_exists}.
%%get_db_params(db) ->
%%  try {ok, zaya_storage:db_params(db)}
%%  catch _:Error ->{error,Error} end.
%%
%%%-----------------------------------------------------------------
%%%% @doc  Get db size.
%%% Returns number of bytes occupied by the db
%%% or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec get_db_size(db :: db()) -> Size :: integer() | not_available().
%%get_db_size(db) ->
%%  zaya_db:get_size(db).
%%
%%%-----------------------------------------------------------------
%%%% @doc  Verify hash values for the node's dbs.
%%%% @end
%%%-----------------------------------------------------------------
%%-spec verify_hash() -> ok | no_return().
%%verify_hash() ->
%%  [ zaya:verify_hash( N ) || N <- get_ready_nodes()],
%%  ok.
%%%-----------------------------------------------------------------
%%%% @doc  Verify hash values for the node's dbs.
%%% As input function gets Name of storage as atom,
%%%% @end
%%%-----------------------------------------------------------------
%%-spec verify_hash(Node :: node()) -> ok | no_return().
%%verify_hash(Node) ->
%%  case node() of
%%    Node -> zaya_storage_srv:verify_hash();
%%    _ -> spawn(Node, fun()->zaya:verify_hash(Node) end)
%%  end.
%%%-----------------------------------------------------------------
%%%% @doc 	Add storage.
%%% It adds a new storage to zaya_schema with creating a new Root db (table)
%%% As input function gets Name of storage as atom and Type as atom.
%%% Returns ok, or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec add_storage(Name :: storage(), Type :: storage_type()) -> ok | {error,any()}.
%%add_storage(Name,Type)->
%%  zaya_storage:add(Name,Type).
%%
%%%-----------------------------------------------------------------
%%%% @doc  Add storage with Options.
%%% Function adds a new storage to zaya_schema with creating a new Root db (table)
%%% As input function gets Name of storage as atom,
%%% Type as atom, and Options as map of
%%% #{
%%%   type:=Type            :: disc | ram | ramdisc
%%%   nodes:=Nodes,         :: list of atom() [node(),..]
%%%   local:=IsLocal,       :: true | false
%%%   limits:=Limits        :: map
%%% }
%%% Options might be used to change default values of nodes.
%%% Returns: ok, or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec add_storage(Name :: storage(), Type :: storage_type(), Params :: db_info()) -> ok | no_return().
%%add_storage(Name,Type,Params)->
%%  zaya_storage:add(Name,Type,Params).
%%
%%%-----------------------------------------------------------------
%%%% @doc 	Remove storage.
%%% Function removes the storage from zaya_schema and deletes all related
%%% dbs (tables)
%%% As input function gets Name of storage as atom
%%% Returns ok, or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec remove_storage(Name :: atom()) -> ok | no_return().
%%remove_storage(Name)->
%%  zaya_storage:remove(Name).
%%
%%%-----------------------------------------------------------------
%%%% @doc  Get default limits for storages.
%%% Function returns actual default limits settings
%%% Returns limits in format:
%%% #{
%%%   0 := BufferLimit      :: integer (MB)
%%%   1 := dbLimit    :: integer (MB)
%%% }
%%%% @end
%%%-----------------------------------------------------------------
%%default_limits()->
%%  zaya_storage:default_limits().
%%
%%%-----------------------------------------------------------------
%%%% @doc  Set default storage limits.
%%% Limits has format:
%%% #{
%%%   0 := BufferLimit      :: integer (MB)
%%%   1 := dbLimit    :: integer (MB)
%%% }
%%% Returns: ok, or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%default_limits( Limits )->
%%  zaya_storage:default_limits( Limits ).
%%
%%%-----------------------------------------------------------------
%%%% @doc  Get storage limits by storage name Storage.
%%% Function returns actual limits settings for the Storage
%%% As input function gets Name of storage as atom,
%%% Returns limits in format:
%%% #{
%%%   0 := BufferLimit      :: integer (MB)
%%%   1 := dbLimit    :: integer (MB)
%%% }
%%%% @end
%%%-----------------------------------------------------------------
%%storage_limits( Storage )->
%%  zaya_storage:storage_limits( Storage ).
%%
%%%-----------------------------------------------------------------
%%%% @doc  Set storage limits by storage name Storage.
%%% Limits has format:
%%% #{
%%%   0 := BufferLimit      :: integer (MB)
%%%   1 := dbLimit    :: integer (MB)
%%% }
%%% Returns: ok, or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%storage_limits( Storage, Limits )->
%%  zaya_storage:storage_limits( Storage, Limits ).
%%
%%%-----------------------------------------------------------------
%%%% @doc
%%%	Stop the zaya
%%% Returns ok, or { error, Reason}.
%%%% @end
%%%-----------------------------------------------------------------
%%-spec stop() -> ok | {error, Reason :: any() }.
%%stop()->
%%  application:stop(zaya).
%%
%%%-----------------------------------------------------------------
%%%% @doc Add db_copy to node.
%%% Function copies the db (table) and puts to Node.
%%% As input function gets Name of db (atom), and Node
%%% Returns ok, or { error, Reason}.
%%%% @end
%%%-----------------------------------------------------------------
%%-spec ADD_DB_copy(db :: db(), Node :: node()) -> ok | { error, Reason :: any() }.
%%ADD_DB_copy(db,Node)->
%%  zaya_storage:ADD_DB_copy( db, Node ).
%%
%%%-----------------------------------------------------------------
%%%% @doc Remove db_copy from node.
%%% Function removes the db (table) from Node.
%%% As input function gets Name of db (atom), and Node
%%% Returns ok, or { error, Reason}.
%%%% @end
%%%-----------------------------------------------------------------
%%-spec REMOVE_DB_COPY(db :: db(), Node :: node()) -> ok | { error, Reason :: any() }.
%%REMOVE_DB_COPY(db,Node)->
%%  zaya_storage:REMOVE_DB_COPY(db,Node).
%%
%%%%---------------------------------------------------------------
%%%%	DATA API
%%%%---------------------------------------------------------------
%%%%=================================================================
%%%%	Read/Write/Delete
%%%%=================================================================
%%
%%%---------------------Read-----------------------------------------
%%%% @doc Read.
%%%% Function reads the value from Storage with Key.
%%% The function needs to be wrapped in transaction.
%%% Returns Value or not_found.
%%%% @end
%%%------------------------------------------------------------------
%%-spec read(Storage :: storage(), Keys :: list(any())) -> KVs :: list(tuple(any(),any())).
%%read(Storage, Keys ) ->
%%    zaya_storage:read(Storage, Keys).
%%
%%%---------------------Write---------------------------------------
%%%% @doc Write.
%%% Function writes the Value to the Storage with Key.
%%% If there is a Key in Storage it just updates,
%%% else it adds new #kv{key:=Key,value:=Value} to the Storage.
%%% The function needs to be wrapped in transaction.
%%% Returns ok or throws Error.
%%%% @end
%%%------------------------------------------------------------------
%%-spec write(Storage :: atom(), KVs :: list(tuple(any(),any()))) -> ok | not_available().
%%write(Storage, KVs)->
%%  zaya_storage:write(Storage, KVs).
%%
%%%---------------------Delete---------------------------------------
%%%% @doc Delete.
%%% Function updates the Value to the '@deleted@' in the Storage with Key,
%%% which will be ignored on read().
%%% The function needs to be wrapped in transaction.
%%% Returns ok or throws Error.
%%%% @end
%%%------------------------------------------------------------------
%%-spec delete(Storage :: atom(), Keys :: any()) -> ok | not_available().
%%delete(Storage, Keys)->
%%  zaya_storage:delete(Storage, Keys).
%%
%%%%=================================================================
%%%%	Iterate
%%%%=================================================================
%%%-----------------------------------------------------------------
%%%% @doc First.
%%% Function gets the first Key of the Storage
%%% As input function gets Name of storage as atom
%%% The function needs to be wrapped in transaction.
%%% Returns Key, or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec first(Storage :: storage()) -> Key :: key() | not_available().
%%first(Storage)->
%%  zaya_storage:first(Storage).
%%
%%%-----------------------------------------------------------------
%%%% @doc	Last.
%%% Function gets the last Key of the Storage
%%% As input function gets Name of storage as atom
%%% The function needs to be wrapped in transaction.
%%% Returns Key, or throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec last(Storage :: storage()) -> Key :: key().
%%last(Storage)->
%%  zaya_storage:last(Storage).
%%
%%%-----------------------------------------------------------------
%%%% @doc	Next.
%%% Function gets the next key of the Storage, from given Key
%%% As input function gets Name of storage as atom and pivot Key
%%% The function needs to be wrapped in transaction.
%%% Returns RetKey, or '$end_of_table' or  throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec next(Storage :: storage(), Key :: key()) -> Key :: key().
%%next( Storage, Key )->
%%  zaya_storage:next( Storage, Key ).
%%
%%%-----------------------------------------------------------------
%%%% @doc	Dirty Next.
%%% Function gets the next key of the Storage, from given Key
%%% As input function gets Name of storage as atom and pivot Key
%%% There is no needs of wrapping in transaction, when using dirty_next
%%% Returns RetKey, or '$end_of_table' or  throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec dirty_next(Storage :: atom(), Key :: any()) -> RetKey :: any() | '$end_of_table' | no_return().
%%dirty_next(Storage,Key)->
%%  zaya_storage:dirty_next(Storage,Key).
%%
%%%-----------------------------------------------------------------
%%%% @doc	Previous.
%%% Function gets the previous key of the Storage, from given Key
%%% As input function gets Name of storage as atom and pivot Key
%%% The function needs to be wrapped in transaction.
%%% Returns RetKey, or '$end_of_table' or  throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec prev(Storage :: atom(), Key :: any()) -> RetKey :: any() | '$end_of_table' | no_return().
%%prev( Storage, Key )->
%%  zaya_storage:prev( Storage, Key ).
%%
%%%-----------------------------------------------------------------
%%%% @doc	Dirty Previous.
%%% Function gets the previous key of the Storage, from given Key
%%% As input function gets Name of storage as atom and pivot Key
%%% There is no needs of wrapping in transaction, when using dirty_previous
%%% Returns RetKey, or '$end_of_table' or  throws Error
%%%% @end
%%%-----------------------------------------------------------------
%%-spec dirty_prev(Storage :: atom(), Key :: any()) -> RetKey :: any() | '$end_of_table' | no_return().
%%dirty_prev(Storage,Key)->
%%  zaya_storage:dirty_prev(Storage,Key).
%%
%%
%%%-----------------------------------------------------------------
%%%% @doc	Dirty Range Select.
%%% Function searches the storage for keys relating to the defined range of keys.
%%% StartKey and EndKey define the range to run trough.
%%% '$start_of_table' and '$end_of_table' are supported.
%%% Returns a list of found items [{ Key, Value}|...]
%%%% @end
%%%-----------------------------------------------------------------
%%-spec dirty_range_select(Storage :: atom(), StartKey :: any(), EndKey ::any() ) -> Items :: list() | no_return().
%%dirty_range_select(Storage, StartKey, EndKey) ->
%%  zaya_storage:dirty_range_select(Storage,StartKey,EndKey).
%%
%%%-----------------------------------------------------------------
%%%% @doc	Dirty Range Select.
%%% Function searches the storage for keys relating to the defined range of keys.
%%% StartKey and EndKey define the interval to run trough.
%%% Limit defines the maximum number of item to return
%%% '$start_of_table' and '$end_of_table' are supported.
%%% Returns a list of found items [{ Key, Value}|...]
%%%% @end
%%%-----------------------------------------------------------------
%%-spec dirty_range_select(Storage :: atom(), StartKey :: any(), EndKey ::any(), Limit :: integer() ) -> Items :: list() | no_return().
%%dirty_range_select(Storage, StartKey, EndKey, Limit) ->
%%  zaya_storage:dirty_range_select(Storage,StartKey,EndKey,Limit).

% Update API docs
% edoc:files(["src/zaya.erl"],[{dir, "doc"}]).