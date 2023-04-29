create table zulrah_scoreboard (
    name String,
    rank Int64,
    score Int64,
    dead Bool,
    page Int64
)
engine = MergeTree
order by name;
