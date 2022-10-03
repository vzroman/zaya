
-module(zaya_transaction).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-define(logModule, zaya_leveldb).

-define(logParams,
  #{
    eleveldb => #{
      %compression_algorithm => todo,
      open_options=>#{
        create_if_missing => false,
        error_if_exists => false,
        %write_buffer_size => todo
        %sst_block_size => todo,
        %block_restart_interval = todo,
        %block_size_steps => todo,
        paranoid_checks => false,
        verify_compactions => false,
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
).

-define(log,{?MODULE,'$log$'}).
-define(transaction,{?MODULE,'$transaction$'}).

%%=================================================================
%%	Environment API
%%=================================================================
-export([
  create_log/1,
  open_log/1
]).

%%=================================================================
%%	Environment
%%=================================================================
create_log( Path )->
  ?logModule:create( ?logParams#{ dir=> Path } ),
  open_log( Path ).

open_log( Path )->
  persistent_term:put(?log, ?logModule:open( ?logParams#{ dir=> Path } )),
  ok.

%%=================================================================
%%	API
%%=================================================================
-export([
  read/3,
  write/3,
  delete/3,

  transaction/1
]).

read(DB, Keys, Lock)->
  todo.

write(DB, KVs, Lock)->
  todo.

delete(DB, Keys, Lock)->
  todo.

transaction(Fun)->
  todo.





