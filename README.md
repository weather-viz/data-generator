I want to see historical weather data for a given location, in an easy-to-grasp format. This tool helps in gathering the data for it.

This tool in active development. Right now, it's able to download weather data from the weather.com API, insert it into a QuestDB instance, and serve some stats via an API.

### How to start a questdb server

```
docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
```

### How to download weather data

```
./generator.py download
```

Help available via the `--help` flag.

The `--server` flag spins up a web server which is used by the frontend to display stats. In future, I plan to generate static JSON files for these API responses, save them in some storage and serve those via frontend - saving me the trouble of maintaining a running backend.