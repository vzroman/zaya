
-module(zaya_util).

-export([
    pretty_size/1,
    pretty_count/1
]).

pretty_size( Bytes )->
    pretty_print([
        {"TB", 2, 40},
        {"GB", 2, 30},
        {"MB", 2, 20},
        {"KB", 2, 10},
        {"B", 2, 0}
    ], Bytes).

pretty_count( Count )->
    pretty_print([
        {"bn", 10, 9},
        {"mn", 10, 6},
        {"ths", 10, 3},
        {"items", 10, 0}
    ], Count).

pretty_print( Units, Value )->
    Units1 = eval_units( Units, round(Value) ),
    Units2 = head_units( Units1 ),
    string:join([ integer_to_list(N) ++" " ++ U || {N,U} <- Units2 ],", ").


eval_units([{Unit, Base, Pow}| Rest], Value )->
    UnitSize = round( math:pow(Base, Pow) ),
    [{ Value div UnitSize, Unit} | eval_units(Rest, Value rem UnitSize)];
eval_units([],_Value)->
    [].

head_units([{N1,U1}|Rest]) when N1 > 0 ->
    case Rest of
        [{N2,U2}|_] when N2 > 0->
            [{N1,U1},{N2,U2}];
        _ ->
            [{N1,U1}]
    end;
head_units([Item])->
    [Item];
head_units([_Item|Rest])->
    head_units(Rest).


