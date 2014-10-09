library(data.table)
library(pipeR)
library(lubridate)
library(reshape2)
library(dplyr)

data.path <- paste0(getwd(), "/ctp/")
dt.rq <- read.csv(paste0(data.path, "data4-renqian.csv"), header=FALSE) %>>% data.table() 
dt.rk <- read.csv(paste0(data.path, "data4-renkun.csv"), header=FALSE) %>>% data.table()
dt.sv <- read.csv(paste0(data.path, "data4-server.csv"), header=FALSE) %>>% data.table()

dt.rq <- dt.rq[, HMS := as.POSIXct(V2, format="%H:%M:%S")]
dt.rk <- dt.rk[, HMS := as.POSIXct(V2, format="%H:%M:%S")]
dt.sv <- dt.sv[, HMS := as.POSIXct(V2, format="%H:%M:%S")]

dt.rq <- dt.rq[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]
dt.rk <- dt.rk[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]
dt.sv <- dt.sv[HMS > as.POSIXct("11:30:00", format="%H:%M:%S"), ]

View( head(dt.rq, 100) )
View( dt.rq[HMS > as.POSIXct("15:00:00", format="%H:%M:%S"), ] )
View( dt.rk[HMS > as.POSIXct("15:00:00", format="%H:%M:%S"), ] )
View( dt.sv[HMS > as.POSIXct("15:00:00", format="%H:%M:%S"), ] )


dt.list <- list(dt.rq = dt.rq, dt.rk = dt.rk, dt.sv = dt.sv )
sapply(dt.list, nrow)
sapply(dt.list, FUN=function(dt) max(dt[, HMS]))

dt.all <- rbind(
  data.table(dt.rq[, list(V2, V3, V4, V5)]),
  data.table(dt.rk[, list(V2, V3, V4, V5)]),
  data.table(dt.sv[, list(V2, V3, V4, V5)])
)

dt.all <- dt.all[, 
                 f := c(rep("rq", nrow(dt.rq)), rep("rk", nrow(dt.rk)), rep("sv", nrow(dt.sv)))]

dt.dcast <- dcast.data.table(dt.all, V2 + V3 + V4 ~ f, value.var="V5")
View( head(dt.dcast, 100) )

#

time.nsame <- dt.dcast[ (dt.dcast[, rq] != dt.dcast[, rk] ) | 
                        (dt.dcast[, rq] != dt.dcast[, sv] ) |
                        (dt.dcast[, sv] != dt.dcast[, rk] ),  ]

View( time.nsame )  # 694

sum(time.nsame[, rq] == 0)   # 185
sum(time.nsame[, rk] == 0)   # 41
sum(time.nsame[, sv] == 0)   # 231


i <- 20

time.nsame[i, ]

View( filter(dt.rq, V2 == time.nsame[i, V2], 
                    V3 == time.nsame[i, V3], 
                    V4 == time.nsame[i, V4]) )
View( filter(dt.rk, V2 == time.nsame[i, V2], 
                    V3 == time.nsame[i, V3], 
                    V4 == time.nsame[i, V4]) )
View( filter(dt.sv, V2 == time.nsame[i, V2], 
                    V3 == time.nsame[i, V3], 
                    V4 == time.nsame[i, V4]) )

#

time.same <- dt.dcast[ (dt.dcast[, rq] == dt.dcast[, rk] ) & 
                       (dt.dcast[, rq] == dt.dcast[, sv] ) &
                       (dt.dcast[, sv] == dt.dcast[, rk] ),  ]

time.same[, recog := paste(V2, V3, V4)]
dt.all[, recog := paste(V2, V3, V4)]

head( dt.all[, recog] )
head( time.same[, recog] )

dt.same <- subset(dt.all, dt.all[, recog] %in% time.same[, recog] )
nrow(dt.all)
nrow(dt.same)
dt.same.dcast <- dcast.data.table(dt.same, V2 + V3 + V4 ~ f, value.var="V5",
                                  fun.aggregate=function(dt) head(dt, 1))



dt.nsame <- filter(dt.same.dcast, rk != rq | rk != sv | rq != sv )
View(dt.nsame)











# dt.dcast.head <- dcast.data.table(dt.all, V2 + V3 + V4 ~ f, value.var="V5",
#                        fun.aggregate = function(dt) head(dt, 1) )
# View( head(dt.dcast.head, 100) )
# head(dt.dcast.head, 100)
# 
# 
# identical(dt.dcast.head[, rq], dt.dcast.head[, rk])
# identical(dt.dcast.head[, rq], dt.dcast.head[, sv])
# identical(dt.dcast.head[, sv], dt.dcast.head[, rk])
# 
# data.nsame <- dt.dcast.head[ (dt.dcast.head$rq != dt.dcast.head$rk ) | 
#                         (dt.dcast.head$rq != dt.dcast.head$sv ) |
#                         (dt.dcast.head$sv != dt.dcast.head$rk ),  ]
# 
# View(data.nsame)   # 291, all in 14:47:00 to 14:49:00
# 
# 
# time.nsame[, V2:V4] %in% data.nsame[, c("V2", "V3", "V4")]
# 
# data.nsame
# 
# i <- 10
# p <- data.nsame[i, ]
# filter(dt.rk, V2 == p[, V2] & V3 == p[, V3] & V4 == p[, V4])

