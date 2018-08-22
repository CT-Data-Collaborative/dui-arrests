library(dplyr)
library(datapkg)
library(tidyr)

##################################################################
#
# Processing Script for DUI Arrests
# Created by Jenna Daly
# On 10/02/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
raw_location <- grep("raw", sub_folders, value=T)
path_to_raw <- (paste0(getwd(), "/", raw_location))
raw_data <- dir(path_to_raw, recursive=T, pattern = "Crime") 

#Create new population data set for all years
source('./scripts/getPopulation.R')

dui_arrests <- data.frame(stringsAsFactors=F)
for (i in 1:length(raw_data)) {
  current_file <- read.csv(paste0(path_to_raw, "/", raw_data[i]), stringsAsFactors = F, header=T, check.names=F)
  names(current_file)[1] <- "Crime"
  #Remove rows without Ages
  current_file <- current_file[grepl("[0-9]", current_file$Crime),]
  #Isolate "DUI" rows
  current_file <- current_file[grep("DUI", current_file$Crime),]
  #convert wide to long
  last_col <- ncol(current_file)
  current_file_long <- gather(current_file, Indicator, Value, 2:last_col, factor_key=TRUE)
  #Assign Age column
  current_file_long$Age <- gsub("([a-zA-Z ]+)(<?[0-9+-]+$)", "\\2", current_file_long$Crime)
  #Remove Ages from Crime column
  current_file_long$Crime <- gsub("[^a-zA-Z]", "", current_file_long$Crime)
  get_year <- as.numeric(substr(unique(unlist(gsub("[^0-9]", "", unlist(raw_data[i])), "")), 1, 4))
  current_file_long$Year <- get_year
  dui_arrests <- rbind(dui_arrests, current_file_long)
}

dui_arrests$Indicator <- as.character(dui_arrests$Indicator)

#Removing values for county (where name is both a county and a town)
years <- c("2010", "2011", "2012", "2013") #Years where indicators are counties not towns
indicators <- c("Hartford", "Windham", "Tolland", "New London", "New Haven", "Litchfield", "Fairfield")
dui_arrests <- dui_arrests[!(dui_arrests$Year %in% years & dui_arrests$Indicator %in% indicators),]

#Fix Indicator names
dui_arrests$Indicator[dui_arrests$Indicator == "CT"] <- "Connecticut"
dui_arrests$Indicator <- gsub(" CSP", "", dui_arrests$Indicator)
dui_arrests$Indicator <- gsub(" PD", "", dui_arrests$Indicator)

#Merge Groton names and Putnam names
dui_arrests$Indicator[which(grepl("Groton", dui_arrests$Indicator))] <- "Groton"
dui_arrests$Indicator[which(grepl("Putnam", dui_arrests$Indicator))] <- "Putnam"

#Merge in FIPS (to remove non-towns)
names(dui_arrests)[names(dui_arrests) == "Indicator"] <- "Town"

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
fips <- (town_fips_dp$data[[1]])

fips <- as.data.frame(fips)

dui_arrests_fips <- merge(dui_arrests, fips, by = "Town", all.y=T)

#Aggregate towns
dui_arrests_fips <- dui_arrests_fips %>% 
  group_by(Year, Age, Town, Crime) %>% 
  mutate(Value = sum(Value))

dui_arrests_fips <- unique(dui_arrests_fips)

# Assign age group flags (one age may belong to mutliple groups)
dui_arrests_fips$`10 years and over flag` <- "TRUE"
dui_arrests_fips$`10 to 20 years flag` <- "FALSE"
dui_arrests_fips$`10 to 17 years flag` <- "FALSE"
dui_arrests_fips$`21 years and over flag` <- "FALSE"
dui_arrests_fips$`18 to 24 years flag` <- "FALSE" 

#Assign flags based on Age column
dui_arrests_fips$`10 years and over flag`[dui_arrests_fips$`Age` == "<10"] <- "FALSE"

x1020 <- c("10-12", "13-14", "15", "16", "17", "18", "19", "20")
dui_arrests_fips$`10 to 20 years flag`[dui_arrests_fips$`Age` %in% x1020] <- "TRUE"

x1017 <- c("10-12", "13-14", "15", "16", "17")
dui_arrests_fips$`10 to 17 years flag`[dui_arrests_fips$`Age` %in% x1017] <- "TRUE"

over20 <- c("21", "22", "23", "24", "25-29", "30-34", "35-39", "40-44", 
            "45-49", "50-54", "55-59", "60-64", "65+")
dui_arrests_fips$`21 years and over flag`[dui_arrests_fips$`Age` %in% over20] <- "TRUE"

x1824 <- c("18", "19", "20", "21", "22", "23", "24")
dui_arrests_fips$`18 to 24 years flag`[dui_arrests_fips$`Age` %in% x1824] <- "TRUE"

#Aggregate age groups based on flag
dui_arrests_calc <- dui_arrests_fips %>% 
  group_by(Town, Year, `10 years and over flag`) %>% 
  mutate(`10 years and over` = ifelse(`10 years and over flag` == "TRUE", sum(Value), 0))

dui_arrests_calc <- dui_arrests_calc %>% 
  group_by(Town, Year, `10 to 20 years flag`) %>% 
  mutate(`10 to 20 years` = ifelse(`10 to 20 years flag` == "TRUE", sum(Value), 0))

dui_arrests_calc <- dui_arrests_calc %>% 
  group_by(Town, Year, `10 to 17 years flag`) %>% 
  mutate(`10 to 17 years` = ifelse(`10 to 17 years flag` == "TRUE", sum(Value), 0))

dui_arrests_calc <- dui_arrests_calc %>% 
  group_by(Town, Year, `21 years and over flag`) %>% 
  mutate(`21 years and over` = ifelse(`21 years and over flag` == "TRUE", sum(Value), 0))

dui_arrests_calc <- dui_arrests_calc %>% 
  group_by(Town, Year, `18 to 24 years flag`) %>% 
  mutate(`18 to 24 years` = ifelse(`18 to 24 years flag` == "TRUE", sum(Value), 0))

#Create total column
dui_arrests_calc <- dui_arrests_calc %>% 
  group_by(Town, Year) %>% 
  mutate(Total = sum(Value))

#Complete df with all totals
dui_arrests_totals <- dui_arrests_calc %>% 
  group_by(Town, Year, FIPS) %>% 
  summarise(`10 years and over` = max(`10 years and over`), 
            `10 to 20 years` = max(`10 to 20 years`), 
            `10 to 17 years` = max(`10 to 17 years`),             
            `21 years and over` = max(`21 years and over`), 
            `18 to 24 years` = max(`18 to 24 years`), 
            `Total` = max(Total))

##########################################################################################################
#Create CT values for 2015 (2015 file does not have CT level values)
CT_2015 <- dui_arrests_calc[dui_arrests_calc$Year == "2015",]

#Add up all totals
CT_2015_calc <- CT_2015 %>% 
  group_by(Age) %>% 
  summarise(`10 years and over` = sum(`10 years and over`), 
            `10 to 20 years` = sum(`10 to 20 years`),             
            `10 to 17 years` = sum(`10 to 17 years`), 
            `21 years and over` = sum(`21 years and over`), 
            `18 to 24 years` = sum(`18 to 24 years`), 
            `Total` = sum(Total))

CT_2015_final <- CT_2015_calc %>% 
  group_by() %>% 
  summarise(`10 years and over` = max(`10 years and over`), 
            `10 to 17 years` = max(`10 to 17 years`), 
            `10 to 20 years` = max(`10 to 20 years`),             
            `21 years and over` = max(`21 years and over`), 
            `18 to 24 years` = max(`18 to 24 years`), 
            `Total` = max(Total))

#Create columns
CT_2015_final$Town <- "Connecticut"
CT_2015_final$FIPS <- "09"
CT_2015_final$Year <- 2015

dui_arrests_totals <- as.data.frame(dui_arrests_totals)

#Merge CT 2015 with rest of data
dui_arrests_totals <- rbind(dui_arrests_totals, CT_2015_final)

#convert wide to long
dui_arrests_totals <- gather(dui_arrests_totals, `Age Range`, Value, 4:9, factor_key=TRUE)
####################################################################################################

## read population data for denominators in rate calculations
pops <- read.csv(paste0(path_to_raw, "/", "populations.csv"), stringsAsFactors = F, header=T, check.names=F)

# Helper function for MOE
calcMOE <- function(x, y, moex, moey) {
  moex2 <- moex^2
  moey2 <- moey^2
  d <- x/y
  d2 <- d^2
  
  radicand <- ifelse(
    moex2 < (d2 * moey2),
    moex2 + (d2 * moey2),
    moex2 - (d2 * moey2)
  )
  
  return(sqrt(radicand)/y)
}

pops$FIPS <- gsub("^", "0", pops$FIPS)

percents <- merge(dui_arrests_totals, pops, by = c("Year", "Age Range", "FIPS"))

#Rates are calculated per 10000
percents <- percents %>% 
  mutate(Pop = (Pop/1e4), 
         MOE = (MOE/1e4))

# calculate rates with population denominators,
# keep MOES, calculating appropriately
percents <- percents %>% 
  mutate(`DUI Arrests` = round((Value / Pop), 2),
         `Margins of Error` = round((calcMOE(Value, Pop, 0, MOE)), 2),
         `Measure Type` = "Rate (per 10,000)")

nulls <- c("Value", "Pop", "MOE")
percents[nulls] <- NULL

# melt percents
percents <- melt(
  percents,
  id.vars = c("Town", "FIPS", "Year", "Age Range", "Measure Type"),
  variable.name = "Variable",
  variable.factor = F,
  value.name = "Value",
  value.factor = F
)

percents$Variable <- as.character(percents$Variable)

## FINAL STEPS
# add extra data
dui_arrests_totals$`Measure Type` <- "Number"
dui_arrests_totals$Variable <- "DUI Arrests"

percents <- as.data.frame(percents)
dui_arrests_totals <- as.data.frame(dui_arrests_totals)

# combine number and rate measures into one dataset
dui_arrests_complete <- rbind(dui_arrests_totals, percents)

#Assign factors for sorting
dui_arrests_complete$`Age Range` <- factor(dui_arrests_complete$`Age Range`, levels = c("Total", "10 years and over", "10 to 17 years", "10 to 20 years", "18 to 24 years", "21 years and over"))

# Order and sort columns
dui_arrests_complete <- dui_arrests_complete %>% 
  select(Town, FIPS, Year, `Age Range`, `Measure Type`, Variable, Value) %>% 
  arrange(Town, Year, `Age Range`, `Measure Type`)

# Write to File
write.table(
  dui_arrests_complete,
  file.path(getwd(), "data", "dui-arrests_2015.csv"),
  sep = ",",
  row.names = F,
  na = "-9999"
)
