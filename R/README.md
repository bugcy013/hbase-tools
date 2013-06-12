HBase "R" Scripts
===========

Sample of R scripts for use with data generated from **hbase-rest-stats.py**. 

----
## Requirements
1. A working HBase REST interface installed on your HBase master
2. HBase 0.94.7 or greater (REST interface changes between 0.94.3 -> 0.94.7)

## How do I use this?
1. Create a CSV file using the **hbase-rest-stats.py** script.

    [user@hostname:~ ]$ ./hbase-rest-stats.py -s hbasemaster -f /tmp/data.csv

2. Open up your 'R' console (Or R-Studio). Enter in the sample R scripts found in r.txt to play / render this data.

## What are some samples?
    install.packages('ggplot2')
    install.packages('plyr')
    library(ggplot2)
    library(plyr)
    data <- read.csv('/tmp/data.csv', header=TRUE)
    table_balance <- ddply(subset(data, table != 'ROOT' & table != 'META'), c('server', 'table'), function(x) c(count=nrow(x)))
    server_balance <- ddply(data, c('server'), function(x) c(count=nrow(x)))

    # Render Region Distribution
    ggplot(server_balance,aes(x=server,y=count,fill=count)) + geom_bar() + theme_bw() + labs(title="Region Distribution", y='regions')  + opts(legend.position="none") + opts(axis.text.x = theme_text(angle = 90, hjust = 0))

    # Render Region Distribution per Table
    ggplot(table_balance,aes(x=server,y=count,fill=count)) + geom_bar(stat="identity") + theme_bw() + labs(title="Region Distribution", y='regions')  + opts(legend.position="none") + opts(axis.text.x = theme_text(angle = 90, hjust = 0)) + scale_fill_gradient("Count", low = "#102E37", high = "#2BBBD8") + facet_wrap(~table, scales="free")

## Screenshots
![Region Distribution](screenshots/region_distribution.png?raw=true)
![Table Distribution](screenshots/table_distribution.png?raw=true)
