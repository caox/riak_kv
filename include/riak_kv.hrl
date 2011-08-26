-type reqid() :: 0..134217727.
-type stream_ref() :: {ok, reqid()}.
-type error() :: {error, term()}.

-type range_limit() :: pos_integer().
-type range_opts() :: [{limit, range_limit()}
                       | stream
                       | {timeout, timeout()}
                       | {client, pid()}].
-type range_result() :: {ok, [riak_object:riak_object()]}.

-define(RANGE_COMPLETE, range_coverage_complete).
-define(RANGE_RESULTS, range_results).
-define(DEFAULT_RANGE_LIMIT, 100).
