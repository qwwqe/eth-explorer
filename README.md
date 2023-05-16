# Ethereum Explorer

## Scraper steps

Let *n* be our "batch size"

1) Get the latest header and set *l* to its number
2) Lookup the last fully fetched block and set *x* to its number, or *l - n* if no blocks have been fetched
3) Lookup the most recent gap and set *a* and *b* to its boundaries, where *a* < *b*
4) Set *d* to *max(0, min(n, l-x))*
5) Set *y* to *max(0, min(n-d, b-a))*
6) Let *p* be the set of block numbers to be fetched this round
7) If *d > 0*, set *p = { l, l - 1, ..., l - d }*
8) If *y > 0*, set *p = p U { b - 1, b - 2, ..., b - y }*
9)  Fetch all the blocks in *p*
10) Return to step 1
