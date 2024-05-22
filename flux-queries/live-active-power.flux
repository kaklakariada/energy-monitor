from(bucket: "<bucket>")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "em")
  |> filter(fn: (r) => r["_field"] == "act_power")
  |> filter(fn: (r) => r["phase"] == "c" or r["phase"] == "b" or r["phase"] == "a")
  |> filter(fn: (r) => r["device"] == "oben" or r["device"] == "unten")
  |> filter(fn: (r) => r["source"] == "live")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "mean")
