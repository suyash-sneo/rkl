# RKL

A CLI (to be TUI) based tool that allows you to write SQL-like queries to read data from Kafka topics and display them in your terminal in table-like format


# Progress

Simple parser is built which can parse
1. SELECT can be key[, value]
2. FROM is the topic name
3. WHERE currently takes one equality condition for JSON field comparison.
4. ORDER BY takes timestamp only but can be followed by ASC/DESC
5. LIMIT takes a number


## TUI
- Main TUI
- Envs TUI


## Test queries:

I've tested the following queries using `cargo run -- --query "<query>" and they seem to be working fine. Displaying the data in a tabular format.

```rkl
SELECT key, value FROM random-data LIMIT 5
```

```rkl
SELECT key, value FROM random-data WHERE value->data->field3 = false LIMIT 5
```

# Tests 

One simple test to validate parsing for a sample query.


