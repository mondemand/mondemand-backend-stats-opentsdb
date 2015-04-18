-module (mondemand_backend_stats_opentsdb).

-behaviour (supervisor).
-behaviour (mondemand_server_backend).
-behaviour (mondemand_backend_stats_handler).

%% mondemand_server_backend callbacks
-export ([ start_link/1,
           process/1,
           required_apps/0,
           type/0
         ]).

%% mondemand_backend_stats_handler callbacks
-export ([ header/0,
           separator/0,
           format_stat/10,
           footer/0,
           handle_response/2
         ]).

%% supervisor callbacks
-export ([init/1]).

-compile({parse_transform, ct_expand}).

%%====================================================================
%% mondemand_server_backend callbacks
%%====================================================================
start_link (Config) ->
  supervisor:start_link ( { local, ?MODULE }, ?MODULE, [Config]).

process (Event) ->
  mondemand_backend_worker_pool_sup:process
    (mondemand_backend_stats_opentsdb_worker_pool, Event).

required_apps () ->
  [ ].

type () ->
  supervisor.

%%====================================================================
%% supervisor callbacks
%%====================================================================
init ([Config]) ->
  Number = proplists:get_value (number, Config, 16), % FIXME: replace default

  { ok,
    {
      {one_for_one, 10, 10},
      [
        { mondemand_backend_stats_opentsdb_worker_pool,
          { mondemand_backend_worker_pool_sup, start_link,
            [ mondemand_backend_stats_opentsdb_worker_pool,
              mondemand_backend_connection,
              Number,
              ?MODULE ]
          },
          permanent,
          2000,
          supervisor,
          [ ]
        }
      ]
    }
  }.

%%====================================================================
%% mondemand_backend_stats_handler callbacks
%%====================================================================
header () -> "".

separator () -> "\n".

format_stat (_Num, _Total, Prefix, ProgId, Host,
             MetricType, MetricName, MetricValue, Timestamp, Context) ->
  ActualPrefix = case Prefix of undefined -> ""; _ -> [ Prefix, "." ] end,
  [ "put ",
    ActualPrefix,
    normalize_metric_name (MetricName)," ",
    io_lib:fwrite ("~b ~b ", [Timestamp, MetricValue]),
    "prog_id=",ProgId," ",
    "host=",Host," ",
    "type=",MetricType,
    case Context of
      [] -> "";
      L -> [" ", mondemand_server_util:join ([[K,"=",V] || {K, V} <- L ], " ")]
    end
  ].

footer () -> "\n".

handle_response (Response, _) ->
  error_logger:info_msg ("~p : got unexpected response ~p",[?MODULE, Response]),
  { 0, undefined }.

%%====================================================================
%% internal functions
%%====================================================================
normalize_metric_name (MetricName) ->
  % limit the character set for metric names to alphanumeric and '_'
  {ok, RE} = ct_expand:term (re:compile ("[^a-zA-Z0-9_]")),
  % any non-alphanumeric or '_' characters are replaced by '_'
  re:replace (MetricName, RE, <<"_">>,[global,{return,binary}]).

