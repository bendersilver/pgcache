
## systend-unit

/etc/systemd/system/pgcache.conf
```sh


[Unit]
Description=sqlite cache
After=syslog.target
After=network.target
After=postgresql.service

Requires=network.target
Requires=postgresql.service

[Service]
StartLimitInterval=5
StartLimitBurst=10
WorkingDirectory=<path bin>
ExecStart=<bin>
KillMode=process

User=<user>

Restart=always
RestartSec=5

TimeoutSec=10

# EnvironmentFile=<env file>
Environment="SOCK=/tmp/pgcache.sock"
# SOCK      - путь к сокету, для http

Environment="PG_URL=postgresql://<user>:<pass>@localhost:5432/<db>"
# PG_URL    - url для поключения к pg. Должна быть настроена логическая репликация. Занимает 2 соединения.

Environment="SLOT=pgcache_slot"
# SLOT      - имя слота репликации

Environment="TABLE=public._replica_rule"
# TABLE     - название таблицы pg где будут хранится правила репликации


[Install]
WantedBy=multi-user.target

```


