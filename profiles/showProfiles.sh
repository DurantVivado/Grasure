go tool pprof -http=:6060 cpu.profile &
go tool pprof -http=:7070 mem.profile &
go tool pprof -http=:9090 blk.profile &

# how to kill a process occupying the port
# lsof -i|grep {port}
# or
# netstat -ap|grep {port}
# and kill it.