# MemcLoad-go


### MemcLoad-go

Parse and load to memcache applications logs.

### Requirements

- Go
- memcache

### Using

Run script:

```
go run memc_load.go
```  

optional arguments:

```      
-t               run protobuf test mode
-l               log path
--dry            run debug mode
--pattern        log path pattern, default value:"./data/appsinstalled/[^.]*.tsv.gz"
--idfa           idfa server address, default value: "127.0.0.1:33013"
--gaid           gaid server address, default value: "127.0.0.1:33014"
--adid           adid server address, default value: "127.0.0.1:33015"
--dvid           dvid server address, default value: "127.0.0.1:33016"
```
