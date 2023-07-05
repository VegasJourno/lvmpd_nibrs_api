#Clear workspace
rm(list=ls())

library(tidyverse)
library(rjson)
library(httr)
library(janitor)
library(googledrive)
library(mailR)

####
####
####

#Turn off Scientific Notation
options(scipen = 999)

#Create master dataframe
lvmpd_nibrs_main <- NULL

#Build sequence of days. This moves backwards from current date.
##So the zero value is current date. Goes back about a month.  
x <- seq(0,31)

#For Loop

for (i in x) {
  
  #Create URL
  ##It wants a range of two days, but I only want 1 day, so I feed in the "day" value twice
  
  url <- paste0("https://services.arcgis.com/jjSk6t82vIntwDbs/arcgis/rest/services/Weekly_Public_Crimes/FeatureServer/0/query?where=%20(DateDif%20%3D%20",
                i,
                "%20OR%20DateDif%20%3D%20",
                i,
                ")%20&outFields=*&outSR=4326&f=json")
  
  Sys.sleep(1)

  #Load in the data via API Call
  url_get <- httr::GET(url)
  
  lvmpd_content <- httr::content(url_get, 
                                 as = "text")
  
  #Start parsing JSON file
  ##Be sure to flatten it
  lvmpd_JSON <- jsonlite::fromJSON(lvmpd_content, 
                                   flatten = TRUE)
  
  #Turn into dataframe
  lvmpd_DF <- (lvmpd_JSON$features) %>% 
    as.data.frame() %>% 
    #Start cleaning column names
    clean_names()
  
  #Finish cleaning column names
  colnames(lvmpd_DF) <- gsub("attributes_", "",
                             colnames(lvmpd_DF))
  
  #Bind the new scrape's data
  lvmpd_nibrs_main <- rbind(lvmpd_nibrs_main, 
                        lvmpd_DF)
  
  #Wait 1 second before running again
  Sys.sleep(1)
}

####
####
####

#Fix the date/time columns
lvmpd_nibrs_main <- lvmpd_nibrs_main %>% 
  #Get only first 10 characters
  mutate(reported_on = substr(reported_on, 1, 10)) %>% 
  #Convert into correct date
  mutate(reported_on = as.POSIXct(as.numeric(reported_on), 
                                    origin="1970-01-01")) %>% 
  #Format to our timezone, includes daylight savings
  mutate(reported_on = format(reported_on, tz="America/Los_Angeles",usetz=TRUE)) %>% 
  mutate(reported_on = ymd_hms(reported_on)) %>% 
  arrange(desc(reported_on))

#Drop the objectid columns
lvmpd_nibrs_main <- lvmpd_nibrs_main %>% 
  select(-c(updated_date, date_dif))

####
####
####
#UPDATE RESULTS ON GITHUB

#Write the CSV name (same as before)
lvmpd_nibrs_all_path <- "data/lvmpd_nibrs_all.csv"

#Load in Rolling File from repository
lvmpd_nibrs_all <- read_csv("data/lvmpd_nibrs_all.csv") %>%
  mutate_all(as.character)

#Make all the data "characters"                      
lvmpd_nibrs_main <- lvmpd_nibrs_main %>%
  mutate_all(as.character)

#Rbind the newest results to it
lvmpd_nibrs_all_new <- rbind(lvmpd_nibrs_all,
                             lvmpd_nibrs_main) %>% 
  #Delete any repeat objects
  distinct(objectid, .keep_all = TRUE)

#Write new CSV for repository
write.csv(lvmpd_nibrs_all_new, 
          lvmpd_nibrs_all_path, 
          row.names=FALSE)

####
####
####
#UPLOAD ROLLING 2023 RESULTS TO GOOGLE DRIVE
DRIVE_JSON <- Sys.getenv("DRIVE_JSON")
DRIVE_FOLDER <- Sys.getenv("DRIVE_FOLDER")

googledrive::drive_auth(path = DRIVE_JSON)
td <- drive_get(DRIVE_FOLDER)

drive_rm("lvmpd_nibrs_all")

drive_put(lvmpd_nibrs_all_path, 
          name =  "lvmpd_nibrs_all", 
          type = "spreadsheet", 
          path=as_id(td))

####
####
####

#Read in the environment Secret objects
GMAIL_SENDER <- Sys.getenv("GMAIL_SENDER")
GMAIL_RECIPIENT <- Sys.getenv("GMAIL_RECIPIENT")
GMAIL_USER <- Sys.getenv("GMAIL_USER")
GMAIL_PASS <- Sys.getenv("GMAIL_PASS")

#Get the DateTime that file was exported
ExportDateTime <- format((Sys.time() - (6*60*60)), 
                         "%Y-%m-%d_%I%p")

#Email notification of success
send.mail(from = GMAIL_SENDER,
          to = GMAIL_SENDER,
          subject = paste0("Github Success: LVMPD NIBRS Export - ", ExportDateTime),
          body = "Github Action ran successfully.",
          smtp = list(host.name = "smtp.gmail.com", port = 465, 
                      user.name = GMAIL_USER, 
                      #Generated app password thru Gmail security settings
                      passwd = GMAIL_PASS, 
                      ssl = TRUE),
          authenticate = TRUE,
          send = TRUE)
