# sqliteserver

Implement MySQL server protocol to expose sqlite database to all MySQL clients.

Users can use any MySQL client to connect to server and consume the database service just like working with MySQL server.

## Usage

Initialize server

```go
svr, err := sqliteserver.NewServer(sqliteserver.ServerConfig{
    Network:  "tcp",
    Address:  fmt.Sprintf("localhost:%d", viper.GetInt("server.port")),
    UserName: viper.GetString("database.user"),
    Password: viper.GetString("database.password"),
})
```

Start server

```go
svr.Start()
```

Close server

```go
svr.Close()
```
