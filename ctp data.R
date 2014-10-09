library(data.table)
library(pipeR)
library(lubridate)
library(reshape2)
library(plyr)

data.path <- paste0(getwd(), "/ctp/")
dt.rq <- read.csv(paste0(data.path, "data4-renqian.csv"), header=FALSE) %>>% data.table() 
dt.rk <- read.csv(paste0(data.path, "data4-renkun.csv"), header=FALSE) %>>% data.table()
dt.sv <- read.csv(paste0(data.path, "data4-server.csv"), header=FALSE) %>>% data.table()

dt.list <- list(dt.rq = dt.rq, dt.rk = dt.rk, dt.sv = dt.sv)

lapply(dt.list, dim)
lapply(dt.list, FUN=function(dt.i) sum(is.na(dt.i)) )

dt.rq <- dt.rq[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]
dt.rk <- dt.rq[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]
dt.sv <- dt.rq[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]

# dt.rq[, HMS :=  as.POSIXct(V2, format="%H:%M:%S") ][, H := hour(HMS)][, M := minute(HMS)][, S := second(HMS)]
# dt.rk[, HMS :=  as.POSIXct(V2, format="%H:%M:%S") ][, H := hour(HMS)][, M := minute(HMS)][, S := second(HMS)]
# dt.sv[, HMS :=  as.POSIXct(V2, format="%H:%M:%S") ][, H := hour(HMS)][, M := minute(HMS)][, S := second(HMS)]

View( head( data.table(dt.rq$H, dt.rq$M, dt.rq$S, 
                       dt.rk$H, dt.rk$M, dt.rk$S, 
                       dt.sv$H, dt.sv$M, dt.sv$S), 1000) )

dt.list <- list(dt.rq = dt.rq, dt.rk = dt.rk, dt.sv = dt.sv)
lapply(dt.list, dim)
lapply(dt.list, FUN=function(dt.i) sum(is.na(dt.i)) )


View( head( data.table(dt.rq$HMS, dt.rk$HMS, dt.sv$HMS ), 1000) )

df.rq <- data.frame(dt.rq)
df.rk <- data.frame(dt.rk)
df.sv <- data.frame(dt.sv)
df.list <- list(df.rq, df.rk, df.sv)

lapply(1:ncol(dt.rq), FUN=function(j)  sum(is.na(df.rq[, j]) ))
lapply(1:ncol(dt.rk), FUN=function(j)  sum(is.na(df.rk[, j]) ))
lapply(1:ncol(dt.sv), FUN=function(j)  sum(is.na(df.sv[, j]) ))

dt.all <- rbind(
        data.table(dt.rq[, list(V2, V3, V4, V5)]),
        data.table(dt.rk[, list(V2, V3, V4, V5)]),
        data.table(dt.sv[, list(V2, V3, V4, V5)])
)

dt.all <- dt.all[, 
                 f := c(rep("rq", nrow(dt.rq)), rep("rk", nrow(dt.rk)), rep("sv", nrow(dt.sv)))]

dt.dcast <- dcast(dt.all, V2 + V3 + V4 ~ f, value.var="V5")

head(dt.dcast)
names(dt.dcast) <- c("V2", "V3", "V4", "rq", "rk", "sv")

View( dt.dcast[100000:1000100, ])
View( dplyr::filter(dt.rq, V2 == "13:30:00", V3 == 0, V4 == "FG501") )

identical(dt.dcast$rq, dt.dcast$rk)
identical(dt.dcast$rq, dt.dcast$sv)

head(dt.all)
head(dt.dcast)

dt.dcast <- dcast(dt.all, V2 + V3 + V4 ~ f, value.var="V5")

dt.dcast2 <- dcast.data.table(dt.all, V2 + V3 + V4 ~ f, value.var="V5",
                              fun.aggregate = function(dt) head(dt, 1) )


View( head(dt.dcast2, 100) )

identical( dt.dcast2[, rq], dt.dcast2[, rk])
identical( dt.dcast2[, rq], dt.dcast2[, sv])

nrow(dt.rq)
nrow(dt.rk)
nrow(dt.sv)

identical(dt.rq[, V2], dt.rk[, V2])
identical(dt.rq[, V2], dt.sv[, V2])





data.path <- paste0(getwd(), "/ctp/")
dt.rq <- read.csv(paste0(data.path, "data4-renqian.csv"), header=FALSE) %>>% data.table() 
dt.rk <- read.csv(paste0(data.path, "data4-renkun.csv"), header=FALSE) %>>% data.table()
dt.sv <- read.csv(paste0(data.path, "data4-server.csv"), header=FALSE) %>>% data.table()

dt.rq <- dt.rq[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]
dt.rk <- dt.rq[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]
dt.sv <- dt.rq[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]

identical(dt.rq, dt.rk)
identical(dt.rq, dt.rk)

