library("DBI", "RSQLite") 
library(tidyverse)


#1.Utworz funkcje: rankAccount <- function(dataFrame,colName,groupName,valueSort,num)
filepath="konta.csv"
sep=","
fileConnection<- file(description = filepath,open = "r")
data<-read.table(fileConnection,header = TRUE,fill=TRUE,sep=sep)

nrow(data)
names(data)
colnames(data)
#nrow(na.omit(data))
rankAccount <- function(dataFrame=data,colName,groupName,valueSort,num){
  if(colName %in% colnames(data))
  {
    data <- data %>% filter(data[[colName]] == groupName)
    if(valueSort %in% colnames(data)){
      data <- data[order(-data[[valueSort]]),]
      data[1:num,]
    }
    else
      cat("Missing or not recognized column name")
    
    
  }
  else
    cat("Missing or not recognized column name")
}

filteredData <- rankAccount(data, "occupation", "KIEROWCA_BUSA", "saldo", 10)
filteredData
close(fileConnection)

#2.Tak jak w 1 tylko z uzyciem datachunku.

rankAccount2 <- function(filepath,colName,groupName,valueSort,num, size, sep){
  fileConnection<- file(description = filepath,open = "r")
  data<-read.table(fileConnection,nrows=size,header = TRUE,fill=TRUE, sep=sep)
  columnsNames<-colnames(data)
  df <- data
  repeat{
    if ( nrow(data)==0){
      close(fileConnection)
      break
    }
    data<-read.table(fileConnection,nrows=size,col.names=columnsNames,fill=TRUE,sep=sep)
    df <- rbind(df,data)
  }  
  
  
  if(colName %in% colnames(df))
  {
    df <- df %>% filter(df[[colName]] == groupName)
    if(valueSort %in% colnames(df)){
      df <- df[order(-df[[valueSort]]),]
      df[1:num,]
    }
    else
      cat("Missing or not recognized column name")
    
    
  }
  else
    cat("Missing or not recognized column name")
}

filteredData <- rankAccount2("konta.csv", "occupation", "KASJER", "saldo", 10, size=1000, sep)
View(filteredData)


#3.SPRAWIDZIC CZY DA SIE ZROBIC TO SAMO W zapytaniu SQL dla takich wartosci jak: tabelaZbazyDanych,occupation, nauczyciel, saldo

readToBase<- function(filepath,dbpath,tablename,header=TRUE,size,sep=",",deleteTable=TRUE){
  ap= !deleteTable
  ov= deleteTable
  fileConnection<- file(description = filepath,open = "r")
  dbConn<-dbConnect(RSQLite::SQLite(),dbpath)
  data<-read.table(fileConnection,nrows=size,header = header,fill=TRUE,sep=sep)
  columnsNames<-names(data)
  dbWriteTable(conn = dbConn,name=tablename,data,append=ap,overwrite=ov)
  repeat{
    if ( nrow(data)==0){
      close(fileConnection)
      dbDisconnect(dbConn)
      break
    }
    data<-read.table(fileConnection,nrows=size,col.names=columnsNames,fill=TRUE,sep=sep)
    dbWriteTable(conn = dbConn,name=tablename,data,append=TRUE,overwrite=FALSE)
  }
}


readToBase("konta.csv","bazaKonta.sqlite","konta",size=1000,)

dbp="bazaKonta.sqlite"
con=dbConnect(RSQLite::SQLite(),dbp)
tablename="konta"
colname = "occupation"
groupname = "\"KASJER\""
valueSort="saldo"
rowNum = 10
rankAccount3<-dbGetQuery(con, 
                        paste0("SELECT * FROM ",tablename," where ",colname," = ",groupname," order by ",valueSort," desc LIMIT ",rowNum,";" ) )




