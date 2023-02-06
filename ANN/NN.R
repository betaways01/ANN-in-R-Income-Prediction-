# ---- Lib ----
library("sqldf")
library(readr)
library(dplyr)
#
library(tidymodels)
library(tidyverse)
library(xgboost)
library(caTools)



# ---- Loading/merge Comp/CRSP ----
crsp <- read.csv("crsp-monthly.csv")
crsp <- crsp[ !(crsp$RET %in% c("", "B", "C")), ]
crsp$PRC <- abs(crsp$PRC)
crsp$RET <- as.numeric(crsp$RET)
crsp$RETX <- as.numeric(crsp$RETX)
compustat <- read.csv("compustat2.csv")
## yearmon to merge with Compustat
to.year <- function (x) {
  x %/% 10000 - 1900
}
to.month <- function (x) {
  (x %/% 100) %% 100 - 1  
}
to.yearmon <- function (x) {
  to.year(x) * 12 + to.month(x)
}
names(compustat)[ names(compustat) == "LPERMNO" ] = "PERMNO"
dups <- duplicated(compustat[,1:3])
compustat <- compustat[!dups,]
crsp$yearmon <- to.yearmon(crsp$date)
compustat$yearmon <- to.yearmon(compustat$datadate)
crsp$MARKETCAP <- crsp$PRC * crsp$SHROUT
compustat = merge(compustat, crsp[c("PERMNO", "yearmon", "MARKETCAP")], 
                  by=c("PERMNO", "yearmon"), all.x = TRUE)
crsp$MARKETCAP <- NULL
## rename
names(compustat)[ names(compustat) == "PERMNO" ] = "PERMNO2"
names(compustat)[ names(compustat) == "yearmon" ] = "yearmon2"
merged <- sqldf("select * from crsp, compustat where crsp.PERMNO = 
compustat.PERMNO2 and crsp.yearmon - compustat.yearmon2 between 4 and 6 order by 
PERMNO, date")
merged$yearmon <- NULL
merged$yearmon2 <- NULL
merged$PERMNO2 <- NULL



# ---- Load/merge macro ----
#Unemployment rate
unrate<- read_csv("UNRATE.csv")
unrate <- unrate[757:888,]

#Cosume price index
CPI <- read_csv("CPIAUCSL.csv")
CPI <- CPI[769:900,]

#Real GDP 
GDP <- read_csv("BBKMGDP.csv")
GDP <- GDP[613:744,]

#Income 
Inc <- read_csv("DSPIC96.csv")
Inc <- Inc[625:756,]

#Consumption
Con <- read_csv("PCE.csv")
Con <- Con[625:756,]  

#Production
Pro <- read_csv("INDPRO.csv")
Pro <- Pro[1105:1236,]

#Expenditure
Ex <- read_csv("PCEPI.csv")
Ex <- Ex[625:756,]

#Interest rate
Ir <- read_csv("INTDSRUSM193N.csv")
Ir <- Ir[733:860,]

Macro <- CPI %>% 
  left_join(unrate, by = "DATE") %>% 
  left_join(GDP, by = "DATE") %>% 
  left_join(Inc, by = "DATE") %>% 
  left_join(Con, by = "DATE") %>% 
  left_join(Pro, by = "DATE") %>% 
  left_join(Ex, by = "DATE") %>% 
  left_join(Ir, by = "DATE")

#Re-name variable
Macro <- Macro %>% 
  rename("Unempl.r" = "UNRATE", "Consumption" = "CPIAUCSL", "Real GDP"= "BBKMGDP","Income" = "DSPIC96",
         "Consuption" = "PCE", "Production" = "INDPRO", "Expenditure" = "PCEPI", "Interest rate" = "INTDSRUSM193N")



# ---- Creating data frame ---- 
BadS <- merged %>% 
  mutate(
    ni = ibmiiq - txtq -xoprq, #Net income
    nicq = ni/ceqq,  # Net income / common equity
    nibeq = ni/seqq, # Net income / Shareholders equity
    debeq = dlttq/seqq, # Debt/ shareholders equity
    caltd = chq/dlttq, # Cash / Long term debt
    deta = dlttq/actq, # Debt / Total Assets
    tlta = ltq/atq, # Total liabilities / Total Assets
    nita = ni/atq, # Net Income / Total Assetes
    cata = chq/atq, # Cash / Total Assets
    prb = PRC/seqq, # Price / Book 
    prs =PRC/saleq, # Price / Sales
    gpm = (revtq - cogsq)/saleq, # Gross profit / Sales
    npm = ibq/saleq, # Income before extra ordinary items / Sales
    cacl = actq/lctq, # Current assets / current liabilities
    cstircl = (cheq + rectq)/lctq #Cash and Short-Term Investments + Receivables ) / Current Liabilities
  )


BadS[mapply(is.infinite, BadS)] <- NA


BadS <- BadS %>% 
  select(
    GVKEY, date, PRC, SHROUT, VOL, RETX, vwretd,
    nicq,nibeq,txtq, debeq, caltd, deta, 
    tlta, nita,cata, prb, prs, gpm, 
    npm, cacl,cstircl 
  ) %>% 
  na.omit()

BadS <- BadS %>% 
  mutate(
    LPRC = lag(PRC, n=4, default = NA),
    LSHROUT = lag(SHROUT, n=4, default = NA),
    LVOL = lag(VOL, n=4, default = NA), 
    LRETX= lag(RETX,n=4, default = NA), 
    Lvwretd =lag(vwretd,n=4,default = NA),
    Lnicq = lag(nicq, n=4, default = NA),
    Lnibeq = lag(nibeq, n=4, default = NA),
    Ltxtq = lag(txtq, n=4, default = NA), 
    Ldebeq = lag(debeq, n=4, default = NA), 
    Lcaltd = lag(caltd, n=4, default = NA), 
    Ldeta = lag(deta, n=4, default = NA), 
    Ltlta = lag(tlta, n=4, default = NA), 
    Lnita = lag(nita, n=4, default = NA),
    Lcata = lag(cata, n=4, default = NA), 
    Lprb = lag(prb, n=4 , default = NA), 
    Lprs = lag(prs, n=4, default = NA), 
    Lgpm = lag(gpm, n=4, default = NA), 
    Lnpm = lag(npm, n=4, default = NA), 
    Lcacl = lag(cacl, n=4, default = NA),
    Lcstircl = lag(cstircl, n=4, default = NA),
    UD = as.factor(ifelse(LPRC - PRC >= 0, 1, 0)))%>% 
  select(GVKEY, PRC, date,LPRC,LSHROUT,LVOL,LRETX,Lvwretd,Lnicq,Lnibeq,Ltxtq,Ldebeq,Lcaltd,Ldeta,Ltlta,
         Lnita,Lcata,Lprb,Lprs,Lgpm,Lnpm,Lcacl,Lcstircl,UD) %>% 
  na.omit()


# I would like you to merge the macro and the data frame on date and 
# include every variable from macor in the final data frame (BadS)
# we merge all observations in BadS, and all in Macro, by date

BadS$date <- as.Date(strptime(BadS$date, format = "%Y%m%d"))
BadS$date <- format(as.Date(BadS$date, "%Y-%m-%d"), "%Y-%m")
Macro$DATE <- format(as.Date(Macro$DATE, "%Y-%m-%d"), "%Y-%m")
Macro<- Macro %>% rename("date" = "DATE")
BadS <- merge(BadS,Macro, by="date", all.x = TRUE)


# ---- Split ----
test <- BadS[BadS$date > 20200000,]
train <- BadS[BadS$date < 20200000,]

#Spliting so same comp end up in same df
testGV <- train[,"GVKEY"]
testGV <- unique(testGV)

split <- sample.split(testGV, SplitRatio = 7/10)
trainingComp <- as.data.frame(subset(testGV, split == TRUE))
valdComp <- as.data.frame(subset(testGV, split == FALSE))         

training <- sqldf("select * from train where GVKEY in trainingComp")
validation <- sqldf("select * from train where GVKEY in valdComp")

# If needed the size of the training-, test- and validation set can be altered 
#to achive a model that runs smoothly and dont need to much computing power

# ---- Neural Network ----
#I would like for you to apply neural network to predict PRC, using the package 
library(neuralnet)
#other libraries:
library(keras)
library(mlbench) # to provide fair performance measures as well as reference implementations,
library(magrittr)

#It would be great if you could add some comments on decisions such as: 
# how many hidden layers/nodes to choose 
# What/why you did to regularization and validate the model 
# How/why did you tune the model
# Etc. 


# The number of hidden neurons should be between the size of the input
# layer and the size of the output layer. The number of hidden neurons should be
# 2/3 the size of the input layer, plus the size of the output layer.

# formatting "UD"
train$UD <- as.numeric(train$UD)
test$UD <- as.numeric(test$UD)

# trying to visualize the neural net
# helpful as a roadmap only.
neural <- neuralnet(PRC ~ LPRC+LSHROUT+LVOL+LRETX+Lvwretd+Lnicq+Lnibeq+
                      Ltxtq+Ldebeq+Lcaltd+Ldeta+Ltlta+Lnita+Lcata+Lprb+
                      Lprs+Lgpm+Lnpm+Lcacl+Lcstircl+UD+Consumption+Unempl.r+
                      Real+GDP+Income+Consuption+Production,
                    data = train,
                    # we will use 2 layers,
                    # 1st layer with (K-1) = 20 neurons and 2nd layer with 7 neurons
                    hidden = c(20,13),
                    linear.output = F,
                    lifesign = 'full',
                    rep=1)
# let's visualize the neural net
plot(neural, col.hidden = 'green', col.hidden.synapse = 'orange',
     show.weights = FALSE, information = FALSE, fill = 'khaki')


# MODELLING
# from observing the data, we have some extreme values.
# thus it is important that we normalize the data, then scale it.
# we normalize around the mean
# we scale around the stadndard deviation

# scale
scale <- apply(train, 2, sd)
# normalize
normalizer <- colMeans(train)
# apply scaling
train <- scale(train, center = normalizer, scale = scale)
test <- scale(test, center = normalizer, scale = scale)

# features and target sets
X_train <- as.matrix(train[,c(4:24)])
y_train <- as.matrix(train[,2])

X_test <- as.matrix(test[,c(4:24)])
y_test <- as.matrix(test[,2])

# instantiating the model
NN <- keras_model_sequential() # has linear stack layers as shown above
# defines the size of the output from the dense layer.
# it represents the dimension of the output vector.
# we need 1 dimension in the output neuron, but initially, 13 -2
# we have 21 inputs
NN %>% 
  layer_dense(units = 21, activation = 'relu', input_shape = c(21)) %>%
  layer_batch_normalization()%>%
  layer_dense(units = 1)

# compiling the model
NN %>% compile(loss = 'mse',# we can also use 'mae'
               optimizer = 'adam',
               metrics = c("accuracy"))

# fitting the NN model
# a smaller batch size is first selected, usually 32
# a 20% validation split is set
NNModel <- NN %>%
  fit(X_train, y_train, epochs = 100, batch_size=32, validation_split = 0.2)

# making prediction
NN %>%evaluate(X_test, y_test)
predictions <- NN %>% predict(X_test)


# checking the comparison between actual and predixt y values
plot(y_test, predictions)


# model tuning
#model %>%
#  layer_dense(units = 100, activation = 'relu', input_shape = c(21)) %>%
#  layer_dropout(rate=0.4)  %>%
#  layer_dense(units = 50, activation = 'relu')  %>%
#  layer_dropout(rate=0.2)  %>%
#  layer_dense(units = 1)


# saving predictions and actual
output_data <- data.frame(y_test, predictions)

