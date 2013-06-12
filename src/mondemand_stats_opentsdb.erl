-module (mondemand_stats_opentsdb).

-include_lib ("lwes/include/lwes.hrl").

-behaviour (gen_server).

%% API
-export ([ start_link/1,
           process/1 ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, {}).

%%====================================================================
%% API
%%====================================================================
start_link (Config) ->
  gen_server:start_link ( { local, ?MODULE }, ?MODULE, Config, []).

process (Event) ->
  gen_server:cast (?MODULE, {process, Event}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init (_Config) ->
  { ok, #state {  } }.

handle_call (Request, From, State) ->
  error_logger:warning_msg ("~p : Unrecognized call ~p from ~p~n",
                            [?MODULE, Request, From]),
  { reply, ok, State }.

handle_cast ({process, Binary}, State) ->

  % deserialize the event as a dictionary
  Event =  lwes_event:from_udp_packet (Binary, dict),

  % grab out the attributes
  #lwes_event { attrs = Data } = Event,

  % here's the timestamp
  Timestamp = dict:fetch (<<"ReceiptTime">>, Data),

  % here's the name of the program which originated the metric
  ProgId = dict:fetch (<<"prog_id">>, Data),

  % here's the host, and the rest of the context as a proplist
  { Host, Context } =
    case mondemand_util:construct_context (Event) of
      [] -> { "unknown", [] };
      C ->
        case lists:keytake (<<"host">>, 1, C) of
          false -> { "unknown", C };
          {value, {<<"host">>, H}, OC } -> {H, OC}
        end
    end,

  Num = dict:fetch (<<"num">>, Data),
  lists:foreach (
    fun(E) ->
      MetricName = dict:fetch (mondemand_util:stat_key (E), Data),
      MetricValue = dict:fetch (mondemand_util:stat_val (E), Data),
      % TODO: write to opentsdb here
      error_logger:info_msg ("~p on ~p sent ~p:~p at ~p with context ~p",
        [ProgId, Host, MetricName, MetricValue, Timestamp, Context])
    end,
    lists:seq (1,Num)
  ),
  {noreply, State};

handle_cast (Request, State) ->
  error_logger:warning_msg ("~p : Unrecognized cast ~p~n",[?MODULE, Request]),
  { noreply, State }.

handle_info (Request, State) ->
  error_logger:warning_msg ("~p : Unrecognized info ~p~n",[?MODULE, Request]),
  {noreply, State}.

terminate (_Reason, #state { }) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.
