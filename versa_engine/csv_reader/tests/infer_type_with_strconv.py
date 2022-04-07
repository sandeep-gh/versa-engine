from strconv import strconv

strconv.unregister_convert('int')
series = ['r', '1', '3.0']
res = strconv.convert_series(series, include_type=True)
types = [_[1] for _ in res]

nseries = ['r', '1.0', '3']
res = strconv.convert_series_with_type(nseries, include_type=True, types=types)
ntypes = [_[1] for _ in res]

print(ntypes)
