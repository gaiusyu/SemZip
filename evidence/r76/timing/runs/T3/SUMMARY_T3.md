# T3 timing summary (MB/s on the pre-registered 20 sampled 100k-line blocks per dataset (NOT whole files))

per dataset: median over repeats of the latest clean (other_cores <= 0.5) terminal result per repeat; geometric mean over datasets; byte-weighted = total bytes / total median seconds

| method | datasets aggregated | encode gmean | decode gmean | encode_native gmean | encode byte-weighted | decode byte-weighted | not lossless (timed only) | incomplete |
|---|---|---|---|---|---|---|---|---|
| semzip | 4/4 | 8.306 | 44.303 |  | 8.528 | 46.258 |  |  |
| delog | 4/4 | 27.505 | 77.176 |  | 28.563 | 81.778 |  |  |
| loglite | 4/4 | 33.009 | 100.958 |  | 33.243 | 106.499 |  |  |
| gzip6 | 4/4 | 89.683 | 168.023 |  | 94.965 | 176.539 |  |  |
| xz6 | 4/4 | 16.399 | 140.976 |  | 16.506 | 149.511 |  |  |
| zstd3 | 4/4 | 129.069 | 190.085 |  | 136.267 | 201.312 |  |  |
| xz9e | 4/4 | 3.299 | 127.365 |  | 3.256 | 134.698 |  |  |
| zstd19 | 4/4 | 6.867 | 149.267 |  | 6.702 | 158.579 |  |  |
| logreducer_r | 3/4 | 1.971 | 4.693 | 7.214 | 1.429 | 2.087 | Spark |  |
| logshrink_r | 2/4 | 0.816 | 1.924 | 1.506 | 0.758 | 1.776 | Spark,Windows |  |

| dataset | method | raw MB | encode MB/s | decode MB/s | encode_native MB/s | lossless |
|---|---|---|---|---|---|---|
| HDFS | semzip | 277.3 | 7.667 | 49.830 |  | True |
| HDFS | delog | 277.3 | 26.769 | 75.664 |  | True |
| HDFS | loglite | 277.3 | 19.799 | 85.956 |  | True |
| HDFS | gzip6 | 277.3 | 87.258 | 182.585 |  | True |
| HDFS | xz6 | 277.3 | 9.953 | 129.566 |  | True |
| HDFS | zstd3 | 277.3 | 142.088 | 196.499 |  | True |
| HDFS | xz9e | 277.3 | 3.772 | 108.119 |  | True |
| HDFS | zstd19 | 277.3 | 5.269 | 157.363 |  | True |
| HDFS | logreducer_r | 277.3 | 3.361 | 11.377 | 8.427 | True |
| HDFS | logshrink_r | 277.3 | 1.224 | 2.933 | 2.237 | True |
| Spark | semzip | 170.4 | 5.463 | 24.234 |  | True |
| Spark | delog | 170.4 | 24.292 | 56.544 |  | True |
| Spark | loglite | 170.4 | 27.768 | 76.740 |  | True |
| Spark | gzip6 | 170.4 | 65.893 | 126.677 |  | True |
| Spark | xz6 | 170.4 | 12.022 | 101.846 |  | True |
| Spark | zstd3 | 170.4 | 91.143 | 137.803 |  | True |
| Spark | xz9e | 170.4 | 2.358 | 91.599 |  | True |
| Spark | zstd19 | 170.4 | 4.608 | 100.560 |  | True |
| Spark | logreducer_r | 170.4 | 2.047 | 5.065 | 4.467 | False |
| Spark | logshrink_r | 170.4 | 0.662 | 1.524 | 1.154 | False |
| Windows | semzip | 465.6 | 9.579 | 57.201 |  | True |
| Windows | delog | 465.6 | 43.828 | 124.640 |  | True |
| Windows | loglite | 465.6 | 71.602 | 153.538 |  | True |
| Windows | gzip6 | 465.6 | 131.067 | 220.265 |  | True |
| Windows | xz6 | 465.6 | 40.939 | 218.629 |  | True |
| Windows | zstd3 | 465.6 | 175.821 | 279.502 |  | True |
| Windows | xz9e | 465.6 | 7.812 | 200.317 |  | True |
| Windows | zstd19 | 465.6 | 22.512 | 241.823 |  | True |
| Windows | logreducer_r | 465.6 | 4.403 | 14.472 | 12.844 | True |
| Windows | logshrink_r | 465.6 | 1.963 | 4.861 | 3.353 | False |
| Thunderbird | semzip | 269.0 | 11.864 | 55.774 |  | True |
| Thunderbird | delog | 269.0 | 20.080 | 66.528 |  | True |
| Thunderbird | loglite | 269.0 | 30.158 | 102.575 |  | True |
| Thunderbird | gzip6 | 269.0 | 85.843 | 156.447 |  | True |
| Thunderbird | xz6 | 269.0 | 14.764 | 136.911 |  | True |
| Thunderbird | zstd3 | 269.0 | 121.880 | 172.499 |  | True |
| Thunderbird | xz9e | 269.0 | 1.706 | 132.645 |  | True |
| Thunderbird | zstd19 | 269.0 | 4.068 | 129.726 |  | True |
| Thunderbird | logreducer_r | 269.0 | 0.518 | 0.628 | 3.468 | True |
| Thunderbird | logshrink_r | 269.0 | 0.545 | 1.262 | 1.014 | True |
