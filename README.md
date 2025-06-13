# 1brc

### 1 Attempt

No concurrency, sequential processing. Processing 100M lines takes 27 seconds.
```
go run main.go data/measurements_100m.txt  23.49s user 5.95s system 107% cpu 27.340 total
```

### 2 Attempt

Added concurrency and batches of lines. Use 1m as batch size and 100 as channel size. Processing 1B lines takes 47 seconds.
```
go run main.go data/measurements_1b.txt  268.61s user 6.97s system 585% cpu 47.036 total
```

### 3 Attempt

Started reading from file in bytes chunks. Use 16MB as most optimal chunk size. Processing 1B lines takes 16.6 seconds.
```
go run main.go ./data/measurements_1b.txt  122.17s user 5.77s system 768% cpu 16.659 total
```

### 4 Attempt

Replaced parsing of string to int with custom parser.
```
go run main.go ./data/measurements_1b.txt  96.44s user 5.59s system 737% cpu 13.840 total
```